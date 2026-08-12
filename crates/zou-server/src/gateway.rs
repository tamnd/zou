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
use crate::forward::{Forwarding, Where};
use crate::tenant::{Found, Registry, Routing};

struct Front {
    routing: Routing,
    registry: Arc<Registry>,
    attached: Arc<Attached>,
    forwarding: Option<Arc<Forwarding>>,
    /// The one project this node serves, when it serves one. Set, and
    /// nothing is parsed out of a request at all: every path is that
    /// project's path and every hostname is its hostname.
    only: Option<String>,
}

/// A router that serves every tenant on a store.
///
/// It has no routes of its own. Every path is a tenant's path, which is
/// what makes the first segment safe to read as a ref, and it is why
/// there is a fallback here and nothing else.
pub fn gateway(routing: Routing, registry: Arc<Registry>, attached: Arc<Attached>) -> Router {
    fleet(routing, registry, attached, None)
}

/// The same for a node that is one of several. `forwarding` is what it
/// does with a tenant another node is writing, and a node without it is
/// a node that believes every tenant is its own, which is true of every
/// single node deployment and of nothing else.
pub fn fleet(
    routing: Routing,
    registry: Arc<Registry>,
    attached: Arc<Attached>,
    forwarding: Option<Arc<Forwarding>>,
) -> Router {
    Router::new()
        .fallback(any(dispatch))
        .with_state(Arc::new(Front {
            routing,
            registry,
            attached,
            forwarding,
            only: None,
        }))
}

/// A node that serves one project and refuses nothing on the way in,
/// which is what a function or a container with one database behind it
/// is.
///
/// The routing is gone rather than configured off: there is no hostname
/// to claim and no path segment to eat, so a url is the same url a
/// hosted project would answer and the client that speaks to it needs
/// nothing said about prefixes. What is left of the front door is the
/// attach, which in this mode has usually already happened.
pub fn only(tenant_ref: String, registry: Arc<Registry>, attached: Arc<Attached>) -> Router {
    Router::new()
        .fallback(any(dispatch))
        .with_state(Arc::new(Front {
            routing: Routing::default(),
            registry,
            attached,
            forwarding: None,
            only: Some(tenant_ref),
        }))
}

/// Bring the one project up before anything asks for it.
///
/// Worth doing only where the wait is somebody else's: a function's
/// initialisation, or a container that is not in a load balancer's
/// rotation until it answers a health check. On the request path this
/// is what the first request pays for anyway.
pub async fn preattach(
    tenant_ref: &str,
    registry: &Registry,
    attached: &Attached,
) -> Result<(), String> {
    let entry = registry
        .get(tenant_ref)
        .await?
        .ok_or_else(|| format!("{tenant_ref} is not a project on this store"))?;
    // The router it built is thrown away and the attach it did is not:
    // what this leaves behind is a postmaster up and a slot filled, and
    // the request that arrives next asks the same manager for the same
    // thing and finds it there.
    let _router = attached.router(&entry).await?;
    Ok(())
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
    // Before the attach, because an attach takes the lease and the
    // whole question here is whether this node may have it. The request
    // that goes to a peer goes as it arrived, path and all, so the peer
    // resolves the same tenant this node just did.
    let mut stale = None;
    if let Some(forwarding) = &front.forwarding {
        match forwarding
            .decide(&found.tenant_ref, req.method(), req.headers())
            .await
        {
            Where::Here => {}
            // A socket is not a read, whatever its method says, and it
            // is not a thing the relay can carry either: forwarding is
            // a request and an answer, and an upgrade is neither. So a
            // node that is not the holder says so, and says it to the
            // one kind of request that would otherwise have been served
            // here and quietly cut off from the rest of the project.
            Where::Stale { .. } | Where::There { .. } if upgrading(&req) => {
                return unavailable("this project's sockets are served by the node holding it");
            }
            Where::Stale { behind } => stale = Some(behind),
            Where::There { endpoint } => return forwarding.relay(&endpoint, req).await,
            Where::Refuse { why, status } => {
                return crate::json_body(status, serde_json::json!({"message": why}));
            }
        }
    }
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
    let mut answer = match router.into_service::<Body>().oneshot(req).await {
        Ok(answer) => answer,
        Err(never) => match never {},
    };
    if let Some(behind) = stale {
        crate::ops::stale_read(behind);
        crate::forward::mark_stale(&mut answer, behind);
    }
    answer
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
    // A node that serves one project skips all three questions. The
    // lookup stays, because the entry is where the secret is and a
    // project can be deleted out from under a running node.
    if let Some(tenant_ref) = &front.only {
        let entry = front.registry.get(tenant_ref).await?;
        return Ok(entry.map(|entry| {
            (
                entry,
                Found {
                    tenant_ref: tenant_ref.clone(),
                    path: path.to_string(),
                },
            )
        }));
    }
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
/// Whether this request is asking to stop being a request, which is
/// what a websocket handshake is. The header is a list, and a client
/// that sends `Upgrade: websocket` sends nothing else in it.
fn upgrading(req: &Request<Body>) -> bool {
    req.headers()
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
}

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

    /// A node that holds nothing: every tenant is written by node-b,
    /// which answers at an address nobody has to be listening on.
    struct Elsewhere {
        published_unix: Option<u64>,
    }

    impl crate::forward::Peers for Elsewhere {
        fn holder(&self, _tenant_ref: &str) -> Result<Option<zou_store::lease::Holder>, String> {
            Ok(Some(zou_store::lease::Holder {
                node: "node-b".to_string(),
                endpoint: Some("http://10.0.0.4:8000".to_string()),
                expires_unix: u64::MAX,
                epoch: 2,
                published_unix: self.published_unix,
            }))
        }
    }

    struct Peer;

    impl crate::forward::Proxy for Peer {
        fn send(&self, call: crate::forward::Call) -> Result<crate::forward::Answer, String> {
            Ok(crate::forward::Answer {
                status: 200,
                headers: vec![("content-type".to_string(), "text/plain".to_string())],
                body: format!("the writer answered {}", call.url).into_bytes(),
            })
        }
    }

    fn one_of_several(
        stale_reads: bool,
        published_unix: Option<u64>,
    ) -> (tempfile::TempDir, Arc<Fake>, Router) {
        let dir = tempfile::tempdir().expect("a directory to write into");
        let store: Arc<dyn CasStore> =
            Arc::from(open_store(&dir.path().to_string_lossy()).expect("a store opens"));
        registry::create(
            store.as_ref(),
            &Tenant::new("acme-prod", &secret("acme-prod"), 1),
        )
        .expect("it registers");
        let backend = Arc::new(Fake::default());
        let attached = Arc::new(Attached::new(backend.clone()));
        let registry = Arc::new(Registry::new(store));
        let holders = crate::forward::Holders::new(Arc::new(Elsewhere { published_unix }));
        let forwarding = crate::forward::Forwarding::new("node-a", holders, Arc::new(Peer))
            .with_stale_reads(stale_reads);
        let routing = Routing {
            domains: vec!["zou.example".to_string()],
            path_prefix: false,
        };
        (
            dir,
            backend,
            fleet(routing, registry, attached, Some(Arc::new(forwarding))),
        )
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

    async fn ask(router: &Router, method: &str, url: &str, key: Option<&str>) -> Response {
        let mut req = Request::builder()
            .method(method)
            .uri(url)
            .header(header::HOST, "acme-prod.zou.example");
        if let Some(key) = key {
            req = req.header("apikey", key);
        }
        router
            .clone()
            .oneshot(req.body(Body::empty()).expect("a request"))
            .await
            .expect("an answer")
    }

    #[tokio::test]
    async fn a_write_for_a_tenant_another_node_holds_never_starts_a_database_here() {
        let (_d, backend, router) = one_of_several(false, None);
        let answer = ask(&router, "POST", "/rest/v1/todos", None).await;
        assert_eq!(answer.status(), StatusCode::OK);
        let body = to_bytes(answer.into_body(), 1 << 20).await.expect("a body");
        assert_eq!(
            String::from_utf8_lossy(&body),
            "the writer answered http://10.0.0.4:8000/rest/v1/todos"
        );
        assert!(
            backend.up.lock().unwrap().is_empty(),
            "the decision comes before the attach, because the attach is what takes the lease"
        );
    }

    /// A websocket for a project this node does not hold, with stale
    /// reads on, which is the arrangement where it would otherwise have
    /// been served: the handshake is a GET, and a GET this node is
    /// allowed to answer from its own copy. Answering it would put the
    /// socket on this node's hub and the rest of the project's sockets
    /// on the holder's, which is two rooms of the same name.
    #[tokio::test]
    async fn a_socket_for_a_tenant_another_node_holds_is_refused_rather_than_split_off() {
        for stale_reads in [true, false] {
            let (_d, backend, router) = one_of_several(stale_reads, None);
            let answer = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/realtime/v1/websocket?vsn=2.0.0")
                        .header(header::HOST, "acme-prod.zou.example")
                        .header("apikey", anon("acme-prod"))
                        .header(header::UPGRADE, "websocket")
                        .header(header::CONNECTION, "Upgrade")
                        .body(Body::empty())
                        .expect("a request"),
                )
                .await
                .expect("an answer");
            assert_eq!(answer.status(), StatusCode::SERVICE_UNAVAILABLE);
            let body = to_bytes(answer.into_body(), 1 << 20).await.expect("a body");
            assert!(
                String::from_utf8_lossy(&body).contains("the node holding it"),
                "{}",
                String::from_utf8_lossy(&body)
            );
            assert!(
                backend.up.lock().unwrap().is_empty(),
                "and no database was started to refuse it"
            );
        }
    }

    #[tokio::test]
    async fn a_stale_read_is_answered_here_and_carries_its_age() {
        let published = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock")
            .as_secs()
            - 3;
        let (_d, backend, router) = one_of_several(true, Some(published));
        let answer = ask(&router, "GET", "/auth/v1/health", Some(&anon("acme-prod"))).await;
        assert_eq!(answer.status(), StatusCode::OK);
        // Read from the clock rather than pinned to the second, because
        // a busy machine can cross one between the publish above and
        // the answer below, and the point of the header is the age
        // rather than the exact number.
        let age: u64 = answer
            .headers()
            .get(crate::forward::STALE_SECONDS)
            .and_then(|v| v.to_str().ok())
            .expect("an answer that did not come from the writer says so")
            .parse()
            .expect("a number of seconds");
        assert!(
            (3..8).contains(&age),
            "the age is the one it was published with, {age}"
        );
        assert_eq!(
            backend.up.lock().unwrap().clone(),
            vec!["acme-prod"],
            "and it is this node that answered it"
        );
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

    /// The same store, served as one project: no domains, no prefix.
    fn single(tenant_ref: &str) -> (tempfile::TempDir, Arc<Fake>, Router) {
        let dir = tempfile::tempdir().expect("a directory to write into");
        let store: Arc<dyn CasStore> =
            Arc::from(open_store(&dir.path().to_string_lossy()).expect("a store opens"));
        for one in ["acme-prod", "beta-co"] {
            registry::create(store.as_ref(), &Tenant::new(one, &secret(one), 1))
                .expect("it registers");
        }
        let backend = Arc::new(Fake::default());
        let attached = Arc::new(Attached::new(backend.clone()));
        let registry = Arc::new(Registry::new(store));
        (
            dir,
            backend,
            only(tenant_ref.to_string(), registry, attached),
        )
    }

    #[tokio::test]
    async fn a_single_tenant_node_answers_whatever_host_it_was_asked_at() {
        let (_d, backend, router) = single("acme-prod");
        for host in ["localhost:54321", "anything.example", "10.0.0.4"] {
            let (status, body) =
                call(&router, host, "/auth/v1/health", Some(&anon("acme-prod"))).await;
            assert_eq!(status, StatusCode::OK, "{host}: {body}");
        }
        assert_eq!(
            backend.up.lock().unwrap().len(),
            1,
            "three requests, one attach"
        );
    }

    #[tokio::test]
    async fn a_single_tenant_node_does_not_read_the_first_path_segment() {
        let (_d, _backend, router) = single("acme-prod");
        // Which is exactly the url a path prefix node would have read a
        // ref out of, and here it is a path that project does not serve.
        let (status, _) = call(
            &router,
            "localhost",
            "/beta-co/auth/v1/health",
            Some(&anon("acme-prod")),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn a_single_tenant_node_still_holds_its_own_projects_key() {
        let (_d, _backend, router) = single("acme-prod");
        let (status, _) = call(
            &router,
            "localhost",
            "/auth/v1/health",
            Some(&anon("beta-co")),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn preattach_brings_the_project_up_before_a_request_does() {
        let dir = tempfile::tempdir().expect("a directory to write into");
        let store: Arc<dyn CasStore> =
            Arc::from(open_store(&dir.path().to_string_lossy()).expect("a store opens"));
        registry::create(
            store.as_ref(),
            &Tenant::new("acme-prod", &secret("acme-prod"), 1),
        )
        .expect("it registers");
        let backend = Arc::new(Fake::default());
        let attached = Arc::new(Attached::new(backend.clone()));
        let registry = Arc::new(Registry::new(store));
        preattach("acme-prod", &registry, &attached)
            .await
            .expect("it comes up");
        assert_eq!(backend.up.lock().unwrap().clone(), vec!["acme-prod"]);

        let router = only("acme-prod".to_string(), registry, attached);
        let (status, _) = call(
            &router,
            "localhost",
            "/auth/v1/health",
            Some(&anon("acme-prod")),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            backend.up.lock().unwrap().len(),
            1,
            "the request found it already up"
        );
    }

    #[tokio::test]
    async fn preattaching_a_project_nobody_registered_says_so() {
        let dir = tempfile::tempdir().expect("a directory to write into");
        let store: Arc<dyn CasStore> =
            Arc::from(open_store(&dir.path().to_string_lossy()).expect("a store opens"));
        let backend = Arc::new(Fake::default());
        let attached = Arc::new(Attached::new(backend.clone()));
        let registry = Arc::new(Registry::new(store));
        let why = preattach("acme-prod", &registry, &attached)
            .await
            .expect_err("there is nothing to attach");
        assert!(why.contains("acme-prod"), "{why}");
        assert!(backend.up.lock().unwrap().is_empty());
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
