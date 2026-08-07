//! The front door of a server that has more than one database behind
//! it.
//!
//! Three steps, in this order, and the order is the design. Resolve the
//! ref out of the host or the path, which costs a string compare. Read
//! the entry, which costs a cached lookup and sometimes one small GET.
//! Attach, which costs a lease, a manifest and a postmaster.
//!
//! Anything that can be refused is refused before the step that would
//! have paid for it. A hostname nobody registered never reaches the
//! store, and a ref nobody registered never reaches an attach, so the
//! cheapest request a stranger can make of this server is also the one
//! they can make the most of.
//!
//! Past those three there is nothing multi tenant left. What answers is
//! the ordinary single tenant router, built for that tenant, with that
//! tenant's secret in it. The surfaces underneath do not know there is
//! more than one project on the node and there is nothing for them to
//! get wrong about it.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode, Uri, header};
use axum::response::Response;
use axum::routing::any;
use tower::ServiceExt as _;

use crate::attach::Attached;
use crate::tenant::{Registry, Routing};

struct Front {
    routing: Routing,
    registry: Arc<Registry>,
    attached: Arc<Attached>,
}

/// A router that serves every tenant on a store.
///
/// It has no routes of its own. Every path is a tenant's path, which is
/// what makes the first segment safe to read as a ref, and it is why
/// there is a fallback here and nothing else.
pub fn gateway(routing: Routing, registry: Arc<Registry>, attached: Arc<Attached>) -> Router {
    Router::new()
        .fallback(any(dispatch))
        .with_state(Arc::new(Front {
            routing,
            registry,
            attached,
        }))
}

async fn dispatch(State(front): State<Arc<Front>>, mut req: Request<Body>) -> Response {
    let host = req
        .headers()
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .map(|h| h.to_string());
    let Some(found) = front.routing.resolve(host.as_deref(), req.uri().path()) else {
        return crate::kong_no_route();
    };
    // A registered ref that will not answer and a ref that was never
    // registered are different failures and are told apart: one is the
    // caller's mistake and the other is ours.
    let entry = match front.registry.get(&found.tenant_ref).await {
        Ok(Some(entry)) => entry,
        Ok(None) => return crate::kong_no_route(),
        Err(e) => {
            log::warn!("registry lookup for {}: {e}", found.tenant_ref);
            return unavailable("the tenant registry could not be read");
        }
    };
    let router = match front.attached.router(&entry).await {
        Ok(router) => router,
        Err(e) => {
            log::warn!("attach {}: {e}", found.tenant_ref);
            return unavailable("the database for this project could not be started");
        }
    };
    if found.path != req.uri().path() {
        let Some(uri) = repath(req.uri(), &found.path) else {
            return crate::kong_no_route();
        };
        *req.uri_mut() = uri;
    }
    match router.into_service::<Body>().oneshot(req).await {
        Ok(answer) => answer,
        Err(never) => match never {},
    }
}

/// The same url with a different path, query kept. None only if the
/// rebuilt url will not parse, which means the path held something a
/// url cannot, and that is a 404 rather than a 500 because it came in
/// off the wire.
fn repath(uri: &Uri, path: &str) -> Option<Uri> {
    let mut parts = uri.clone().into_parts();
    let rebuilt = match uri.query() {
        Some(query) => format!("{path}?{query}"),
        None => path.to_string(),
    };
    parts.path_and_query = Some(rebuilt.parse().ok()?);
    Uri::from_parts(parts).ok()
}

/// What a caller is told when the failure is this server's. 503 rather
/// than 500 because both of these are worth retrying, and neither says
/// which tenant or which store, because the person holding an anon key
/// for one project is not owed the operational state of the node.
fn unavailable(why: &str) -> Response {
    crate::json_body(
        StatusCode::SERVICE_UNAVAILABLE,
        serde_json::json!({"message": why}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attach::Backend;
    use crate::{Config, jwt};
    use axum::body::to_bytes;
    use std::sync::Mutex;
    use zou_store::registry::{self, Tenant};
    use zou_store::{CasStore, open_store};

    /// Every tenant gets a router with its own secret and no pool, so
    /// the gate is real and the database is not there to be needed.
    #[derive(Default)]
    struct Fake {
        up: Mutex<Vec<String>>,
    }

    impl Backend for Fake {
        fn up(&self, entry: &Tenant) -> Result<Config, String> {
            self.up.lock().unwrap().push(entry.tenant_ref.clone());
            Ok(Config {
                jwt_secret: entry.jwt_secret.as_bytes().to_vec(),
                ..Config::default()
            })
        }
        fn down(&self, _tenant_ref: &str) {}
    }

    fn secret(tenant_ref: &str) -> String {
        format!("{tenant_ref}-secret-of-at-least-32-characters-long")
    }

    fn front(routing: Routing) -> (tempfile::TempDir, Arc<Fake>, Router) {
        let dir = tempfile::tempdir().expect("a directory to write into");
        let store: Arc<dyn CasStore> =
            Arc::from(open_store(&dir.path().to_string_lossy()).expect("a store opens"));
        for tenant_ref in ["acme-prod", "beta-co"] {
            registry::create(
                store.as_ref(),
                &Tenant::new(tenant_ref, &secret(tenant_ref), 1),
            )
            .expect("it registers");
        }
        let backend = Arc::new(Fake::default());
        let attached = Arc::new(Attached::new(backend.clone()));
        let registry = Arc::new(Registry::new(store));
        (dir, backend, gateway(routing, registry, attached))
    }

    fn hosted() -> (tempfile::TempDir, Arc<Fake>, Router) {
        front(Routing {
            domains: vec!["zou.example".to_string()],
            path_prefix: false,
        })
    }

    async fn call(
        router: &Router,
        host: &str,
        url: &str,
        key: Option<&str>,
    ) -> (StatusCode, String) {
        let mut req = Request::builder().uri(url).header(header::HOST, host);
        if let Some(key) = key {
            req = req.header("apikey", key);
        }
        let answer = router
            .clone()
            .oneshot(req.body(Body::empty()).expect("a request"))
            .await
            .expect("an answer");
        let status = answer.status();
        let body = to_bytes(answer.into_body(), 1 << 20).await.expect("a body");
        (status, String::from_utf8_lossy(&body).to_string())
    }

    fn anon(tenant_ref: &str) -> String {
        jwt::mint(&jwt::key_claims("anon"), secret(tenant_ref).as_bytes())
    }

    #[tokio::test]
    async fn a_request_reaches_the_tenant_its_host_names() {
        let (_d, backend, router) = hosted();
        let (status, body) = call(
            &router,
            "acme-prod.zou.example",
            "/auth/v1/health",
            Some(&anon("acme-prod")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(backend.up.lock().unwrap().clone(), vec!["acme-prod"]);
    }

    #[tokio::test]
    async fn one_projects_key_does_not_open_another() {
        let (_d, _backend, router) = hosted();
        let (status, _) = call(
            &router,
            "beta-co.zou.example",
            "/auth/v1/health",
            Some(&anon("acme-prod")),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "each tenant's router carries that tenant's secret and nothing else"
        );
        let (status, _) = call(
            &router,
            "beta-co.zou.example",
            "/auth/v1/health",
            Some(&anon("beta-co")),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn a_host_nobody_registered_never_reaches_a_database() {
        let (_d, backend, router) = hosted();
        for host in ["zou.example", "nobody.zou.example", "acme-prod.evil.test"] {
            let (status, body) = call(&router, host, "/auth/v1/health", None).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{host}");
            assert!(body.contains("no Route matched"), "{host}: {body}");
        }
        assert!(
            backend.up.lock().unwrap().is_empty(),
            "nothing was started for any of them"
        );
    }

    #[tokio::test]
    async fn a_path_prefix_comes_off_before_the_tenant_sees_the_url() {
        let (_d, _backend, router) = front(Routing {
            domains: Vec::new(),
            path_prefix: true,
        });
        let (status, body) = call(
            &router,
            "zou.example",
            "/acme-prod/auth/v1/health",
            Some(&anon("acme-prod")),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "the tenant's own router was asked for /auth/v1/health: {body}"
        );
    }

    #[tokio::test]
    async fn a_query_survives_the_rewrite() {
        let (_d, _backend, router) = front(Routing {
            domains: Vec::new(),
            path_prefix: true,
        });
        // The apikey in the url rather than the header, which is the
        // spelling a browser navigation uses and the one that proves
        // the query string made it across.
        let (status, body) = call(
            &router,
            "zou.example",
            &format!("/acme-prod/auth/v1/health?apikey={}", anon("acme-prod")),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
}
