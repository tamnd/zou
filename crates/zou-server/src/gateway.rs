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
use zou_store::registry::Tenant;

use crate::attach::Attached;
use crate::tenant::{Found, Registry, Routing};

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
    let (entry, found) = match tenant(&front, host.as_deref(), req.uri().path()).await {
        Ok(Some(both)) => both,
        Ok(None) => return crate::kong_no_route(),
        Err(e) => {
            log::warn!("registry lookup: {e}");
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

/// Which tenant, in the order the answers get more expensive.
///
/// A label under a serve domain is a parse and costs nothing, so it is
/// first. A custom hostname is a lookup, so it is second, and it is
/// only reached by a host the serve domains did not claim, which means
/// adding custom domains to a fleet costs its existing tenants nothing.
/// A path prefix is a parse too, but it is last, because a request that
/// named a tenant by host has already said which one it means and a
/// path segment does not get to argue.
///
/// None is nobody, and nobody is a 404 without a database being
/// started for it.
async fn tenant(
    front: &Front,
    host: Option<&str>,
    path: &str,
) -> Result<Option<(Tenant, Found)>, String> {
    if let Some(tenant_ref) = host.and_then(|h| front.routing.label(h)) {
        let entry = front.registry.get(&tenant_ref).await?;
        return Ok(entry.map(|entry| {
            (
                entry,
                Found {
                    tenant_ref,
                    path: path.to_string(),
                },
            )
        }));
    }
    if let Some(host) = host
        && let Some(entry) = front.registry.by_host(host).await?
    {
        let found = Found {
            tenant_ref: entry.tenant_ref.clone(),
            path: path.to_string(),
        };
        return Ok(Some((entry, found)));
    }
    let Some(found) = front.routing.segment(path) else {
        return Ok(None);
    };
    let entry = front.registry.get(&found.tenant_ref).await?;
    Ok(entry.map(|entry| (entry, found)))
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
    async fn a_project_on_its_own_domain_is_found_by_lookup() {
        let (dir, backend, router) = hosted();
        let store: Arc<dyn CasStore> =
            Arc::from(open_store(&dir.path().to_string_lossy()).expect("a store opens"));
        registry::add_host(store.as_ref(), "beta-co", "api.example.com").expect("it claims one");
        let (status, body) = call(
            &router,
            "api.example.com",
            "/auth/v1/health",
            Some(&anon("beta-co")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(backend.up.lock().unwrap().clone(), vec!["beta-co"]);
        let (status, _) = call(
            &router,
            "api.example.com",
            "/auth/v1/health",
            Some(&anon("acme-prod")),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "a custom domain is the same tenant with the same secret, not a looser door"
        );
    }

    #[tokio::test]
    async fn a_domain_nobody_claimed_is_still_nobody() {
        let (_d, backend, router) = hosted();
        let (status, _) = call(&router, "api.example.com", "/auth/v1/health", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(backend.up.lock().unwrap().is_empty());
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
