//! Which tenant a request is for, and what is known about it.
//!
//! Two ways in, and a server may have both on at once.
//!
//! ```text
//! acme-prod.zou.example/rest/v1/todos   the host names the tenant
//! zou.example/acme-prod/rest/v1/todos   the first path segment does
//! ```
//!
//! Hosts are how this is meant to be deployed, because a tenant that
//! owns a hostname owns an origin, and an origin is what cookies, CORS
//! and every browser security boundary are drawn around. Two tenants
//! sharing an origin share all of that, which is a thing nobody wants
//! to find out later.
//!
//! Path prefixes exist anyway, because a wildcard certificate is not
//! always available and a laptop has no DNS. When they are on, the
//! first segment is always the ref, with no exceptions carved out for
//! `/rest` or `/auth`: a server routing by path has no surface of its
//! own at the root, so there is nothing for a ref to collide with, and
//! a rule with no exceptions is a rule nobody has to remember.
//!
//! What resolution does not do is decide whether the request may
//! proceed. It produces a ref and a rewritten path. The gate that
//! follows reads the entry's secret and checks the apikey, and only a
//! request that passes both is worth attaching a database for.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use zou_store::CasStore;
use zou_store::registry::{self, Tenant};

/// How a server finds the tenant in a request. Neither field set is a
/// single tenant server, where every request is for the one database
/// and nothing is parsed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Routing {
    /// Domains whose immediate subdomains are refs. `zou.example` here
    /// routes `acme-prod.zou.example` to `acme-prod`, and routes
    /// nothing else: two labels deep is not a ref, and neither is the
    /// bare domain.
    pub domains: Vec<String>,
    /// Whether the first path segment names the tenant.
    pub path_prefix: bool,
}

/// A request's tenant and the path with the routing part taken off, so
/// what reaches the router is the url the tenant's own surface expects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Found {
    pub tenant_ref: String,
    pub path: String,
}

impl Routing {
    /// Whether this server routes at all.
    pub fn multi_tenant(&self) -> bool {
        !self.domains.is_empty() || self.path_prefix
    }

    /// The tenant a request is for, from its Host header and its path.
    /// None means no rule matched, which the caller answers rather than
    /// guesses at: a request to a hostname this server has never heard
    /// of is not a request for whichever tenant happens to be first.
    ///
    /// Host wins over path when both would match, because a host is the
    /// stronger statement and because a server that has both on is one
    /// where paths are the fallback for clients that cannot do hosts.
    ///
    /// This is the whole answer only for a server with no custom
    /// hostnames on it. A project on its own domain is resolved out of
    /// the registry instead, which is a lookup rather than a parse, so
    /// the front door calls [`Routing::label`] and [`Routing::segment`]
    /// with that lookup in between them.
    pub fn resolve(&self, host: Option<&str>, path: &str) -> Option<Found> {
        if let Some(tenant_ref) = host.and_then(|h| self.label(h)) {
            return Some(Found {
                tenant_ref,
                path: path.to_string(),
            });
        }
        self.segment(path)
    }

    /// The ref a host names, if any of the serve domains claim it.
    pub fn label(&self, host: &str) -> Option<String> {
        let host = bare_host(host);
        let found = self.domains.iter().find_map(|domain| {
            let domain = domain.trim_start_matches('.').trim_end_matches('.');
            host.strip_suffix(domain)?.strip_suffix('.')
        })?;
        // One label, not two: `a.b.zou.example` is not tenant `a.b`,
        // and it is not tenant `b` either, it is a name nobody
        // registered. check_ref settles both, since a ref cannot hold a
        // dot.
        registry::check_ref(found).ok()?;
        Some(found.to_string())
    }

    /// The ref a path names, and the path without it. None when the
    /// server does not route by path at all, so a caller can ask this
    /// without checking first.
    pub fn segment(&self, path: &str) -> Option<Found> {
        if !self.path_prefix {
            return None;
        }
        let rest = path.strip_prefix('/')?;
        let (first, rest) = match rest.split_once('/') {
            Some((first, rest)) => (first, rest),
            None => (rest, ""),
        };
        // Refused here rather than looked up, so that a browser asking
        // for /favicon.ico is a parse failure and not a request to the
        // object store.
        registry::check_ref(first).ok()?;
        Some(Found {
            tenant_ref: first.to_string(),
            path: format!("/{rest}"),
        })
    }
}

/// A Host header down to the name: no port, no trailing dot, lower
/// case. Ports because the header carries the one the client dialled,
/// trailing dots because a fully qualified name is the same name, and
/// case because DNS does not have any.
pub fn bare_host(host: &str) -> String {
    let host = host.trim();
    // An IPv6 literal is bracketed and full of colons, so the port is
    // only ever what follows the last one outside the brackets.
    let name = match host.rsplit_once(':') {
        Some((name, port)) if !name.ends_with(']') && port.chars().all(|c| c.is_ascii_digit()) => {
            name
        }
        _ => host,
    };
    name.trim_end_matches('.').to_ascii_lowercase()
}

/// How long a tenant entry is believed for. Entries change when a
/// secret is rotated or a project is deleted, which is rare, and the
/// cost of being a minute late to either is small next to a store
/// round trip on every request.
pub const TTL: Duration = Duration::from_secs(60);

/// How long a missing tenant is believed for, which is shorter for one
/// reason: a miss is what someone probing hostnames generates, and a
/// miss is also what the seconds between `zou tenant create` and the
/// first request look like. A minute of 404 after creating a project
/// reads as a bug.
pub const MISS_TTL: Duration = Duration::from_secs(5);

/// How many entries a node keeps. A registry may hold a hundred
/// thousand tenants; a node serves a few thousand at a time.
pub const CAPACITY: usize = 4096;

struct Cached {
    entry: Option<Tenant>,
    until: Instant,
    /// When it was put here, counted rather than clocked, so that no
    /// two entries tie and eviction always makes the room it meant to.
    seq: u64,
}

/// The registry with a memory. Every request needs the entry, and
/// without this every request is a GET against the object store, which
/// would put a network round trip in front of the apikey check and make
/// the cheap path the slow one.
pub struct Registry {
    store: Arc<dyn CasStore>,
    ttl: Duration,
    miss_ttl: Duration,
    capacity: usize,
    seen: RwLock<HashMap<String, Cached>>,
    next: std::sync::atomic::AtomicU64,
}

impl Registry {
    pub fn new(store: Arc<dyn CasStore>) -> Registry {
        Registry {
            store,
            ttl: TTL,
            miss_ttl: MISS_TTL,
            capacity: CAPACITY,
            seen: RwLock::new(HashMap::new()),
            next: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// For tests, and for an operator who wants a shorter leash.
    pub fn with_ttl(mut self, ttl: Duration, miss_ttl: Duration) -> Registry {
        self.ttl = ttl;
        self.miss_ttl = miss_ttl;
        self
    }

    pub fn with_capacity(mut self, capacity: usize) -> Registry {
        self.capacity = capacity.max(1);
        self
    }

    /// The entry for a ref, from memory when it is fresh there.
    ///
    /// Two requests for the same cold ref both fetch it. Single
    /// flighting them would save one GET of one small object and cost a
    /// lock held across the network, which is the wrong trade for the
    /// thing sitting in front of every request.
    pub async fn get(&self, tenant_ref: &str) -> Result<Option<Tenant>, String> {
        let wanted = tenant_ref.to_string();
        self.cached(tenant_ref.to_string(), move |store| {
            registry::get(store, &wanted).map_err(|e| e.to_string())
        })
        .await
    }

    /// The entry a custom hostname points at, which is two GETs cold,
    /// the alias and then the entry, and one cached read warm. Both
    /// steps are cached under the host rather than only the second, so
    /// that a project on its own domain costs the same per request as
    /// one on a serve domain.
    pub async fn by_host(&self, host: &str) -> Result<Option<Tenant>, String> {
        let host = bare_host(host);
        let wanted = host.clone();
        self.cached(format!("host:{host}"), move |store| {
            let Some(tenant_ref) = registry::host_ref(store, &wanted).map_err(|e| e.to_string())?
            else {
                return Ok(None);
            };
            registry::get(store, &tenant_ref).map_err(|e| e.to_string())
        })
        .await
    }

    /// One cache under two key spaces, refs and `host:` names, which
    /// cannot collide because a ref holds neither a colon nor a dot.
    /// One map means one bound and one eviction rather than two of each
    /// that have to be reasoned about together.
    async fn cached<F>(&self, key: String, fetch: F) -> Result<Option<Tenant>, String>
    where
        F: FnOnce(&dyn CasStore) -> Result<Option<Tenant>, String> + Send + 'static,
    {
        let now = Instant::now();
        if let Some(cached) = self.seen.read().await.get(&key)
            && cached.until > now
        {
            crate::ops::lookup(true);
            return Ok(cached.entry.clone());
        }
        crate::ops::lookup(false);
        let store = self.store.clone();
        let entry = tokio::task::spawn_blocking(move || fetch(store.as_ref()))
            .await
            .map_err(|e| format!("registry lookup: {e}"))??;
        let until = Instant::now()
            + match entry.is_some() {
                true => self.ttl,
                false => self.miss_ttl,
            };
        let seq = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut seen = self.seen.write().await;
        seen.insert(
            key,
            Cached {
                entry: entry.clone(),
                until,
                seq,
            },
        );
        if seen.len() > self.capacity {
            evict(&mut seen, self.capacity, now);
        }
        Ok(entry)
    }

    /// Forget one ref, for the node that just changed it and should not
    /// wait out its own ttl.
    pub async fn forget(&self, tenant_ref: &str) {
        self.seen.write().await.remove(tenant_ref);
    }

    /// Forget one custom hostname.
    pub async fn forget_host(&self, host: &str) {
        self.seen
            .write()
            .await
            .remove(&format!("host:{}", bare_host(host)));
    }

    #[cfg(test)]
    async fn len(&self) -> usize {
        self.seen.read().await.len()
    }
}

/// Make room: expired entries first, then the oldest fetched, down to
/// seven eighths of capacity so this runs once per eighth of a cache
/// rather than once per insert.
///
/// First in first out, not least recently used. The difference is one
/// extra GET for a hot entry evicted a few seconds early, against
/// writing a last use on a path that otherwise only takes a read lock,
/// and every entry is refetched on its ttl regardless.
fn evict(seen: &mut HashMap<String, Cached>, capacity: usize, now: Instant) {
    seen.retain(|_, cached| cached.until > now);
    let target = capacity - capacity / 8;
    if seen.len() <= target {
        return;
    }
    let mut order: Vec<u64> = seen.values().map(|cached| cached.seq).collect();
    order.sort_unstable();
    let cutoff = order[seen.len() - target];
    seen.retain(|_, cached| cached.seq >= cutoff);
}

#[cfg(test)]
mod tests {
    use super::*;
    use zou_store::open_store;

    fn hosts(domains: &[&str]) -> Routing {
        Routing {
            domains: domains.iter().map(|d| d.to_string()).collect(),
            path_prefix: false,
        }
    }

    fn paths() -> Routing {
        Routing {
            domains: Vec::new(),
            path_prefix: true,
        }
    }

    fn found(tenant_ref: &str, path: &str) -> Option<Found> {
        Some(Found {
            tenant_ref: tenant_ref.to_string(),
            path: path.to_string(),
        })
    }

    #[test]
    fn a_subdomain_of_a_serve_domain_is_a_tenant() {
        let routing = hosts(&["zou.example"]);
        assert_eq!(
            routing.resolve(Some("acme-prod.zou.example"), "/rest/v1/todos"),
            found("acme-prod", "/rest/v1/todos"),
            "the path is untouched, only the host was read"
        );
    }

    #[test]
    fn a_host_is_read_the_way_dns_reads_it() {
        let routing = hosts(&["zou.example"]);
        for host in [
            "ACME-PROD.ZOU.EXAMPLE",
            "acme-prod.zou.example:8443",
            "acme-prod.zou.example.",
            "  acme-prod.zou.example  ",
        ] {
            assert_eq!(
                routing.resolve(Some(host), "/x"),
                found("acme-prod", "/x"),
                "{host}"
            );
        }
    }

    #[test]
    fn a_host_that_is_not_one_label_under_a_serve_domain_is_nobody() {
        let routing = hosts(&["zou.example"]);
        for host in [
            "zou.example",
            "a.b.zou.example",
            "acme-prod.zou.example.evil.test",
            "acme-prodzou.example",
            "notzou.example",
            "-acme.zou.example",
            "127.0.0.1:5432",
            "[::1]:8443",
        ] {
            assert_eq!(routing.resolve(Some(host), "/x"), None, "{host}");
        }
    }

    #[test]
    fn a_serve_domain_is_taken_however_it_was_spelled() {
        for spelling in ["zou.example", ".zou.example", "zou.example."] {
            assert_eq!(
                hosts(&[spelling]).resolve(Some("acme-prod.zou.example"), "/x"),
                found("acme-prod", "/x"),
                "{spelling}"
            );
        }
    }

    #[test]
    fn any_of_the_serve_domains_will_do() {
        let routing = hosts(&["zou.example", "db.other.test"]);
        assert_eq!(
            routing.resolve(Some("acme-prod.db.other.test"), "/x"),
            found("acme-prod", "/x")
        );
    }

    #[test]
    fn the_first_path_segment_is_the_tenant_and_comes_off() {
        let routing = paths();
        assert_eq!(
            routing.resolve(None, "/acme-prod/rest/v1/todos"),
            found("acme-prod", "/rest/v1/todos")
        );
        assert_eq!(
            routing.resolve(None, "/acme-prod/"),
            found("acme-prod", "/")
        );
        assert_eq!(routing.resolve(None, "/acme-prod"), found("acme-prod", "/"));
    }

    #[test]
    fn a_first_segment_that_cannot_be_a_ref_is_not_looked_up() {
        let routing = paths();
        for path in ["/", "", "/favicon.ico", "/-nope/x", "/UPPER/x", "//rest/v1"] {
            assert_eq!(routing.resolve(None, path), None, "{path}");
        }
    }

    #[test]
    fn nothing_routes_when_nothing_is_configured() {
        let routing = Routing::default();
        assert!(!routing.multi_tenant());
        assert_eq!(routing.resolve(Some("anything.test"), "/acme-prod/x"), None);
    }

    #[test]
    fn the_host_wins_when_both_could_answer() {
        let routing = Routing {
            domains: vec!["zou.example".to_string()],
            path_prefix: true,
        };
        assert_eq!(
            routing.resolve(Some("acme-prod.zou.example"), "/rest/v1/todos"),
            found("acme-prod", "/rest/v1/todos"),
            "the path segment is a path segment, not a second ref"
        );
        assert_eq!(
            routing.resolve(Some("zou.example"), "/acme-prod/rest/v1/todos"),
            found("acme-prod", "/rest/v1/todos"),
            "and the path is still there for a host that names nobody"
        );
    }

    fn store() -> (tempfile::TempDir, Arc<dyn CasStore>) {
        let dir = tempfile::tempdir().expect("a directory to write into");
        let store: Arc<dyn CasStore> =
            Arc::from(open_store(&dir.path().to_string_lossy()).expect("a store opens"));
        (dir, store)
    }

    fn register(store: &dyn CasStore, tenant_ref: &str, secret: &str) {
        registry::create(store, &Tenant::new(tenant_ref, secret, 1)).expect("it registers");
    }

    #[tokio::test]
    async fn an_entry_is_read_once_and_then_remembered() {
        let (_d, store) = store();
        register(store.as_ref(), "acme-prod", "first");
        let cache = Registry::new(store.clone());
        assert_eq!(
            cache.get("acme-prod").await.unwrap().unwrap().jwt_secret,
            "first"
        );
        // Changed underneath, which a fresh read would see and a cached
        // one must not, or there was no cache.
        registry::delete(store.as_ref(), "acme-prod").expect("it unregisters");
        register(store.as_ref(), "acme-prod", "second");
        assert_eq!(
            cache.get("acme-prod").await.unwrap().unwrap().jwt_secret,
            "first"
        );
        cache.forget("acme-prod").await;
        assert_eq!(
            cache.get("acme-prod").await.unwrap().unwrap().jwt_secret,
            "second"
        );
    }

    #[tokio::test]
    async fn a_ref_nobody_registered_is_remembered_as_missing_and_then_looked_at_again() {
        let (_d, store) = store();
        let cache = Registry::new(store.clone())
            .with_ttl(Duration::from_secs(60), Duration::from_millis(1));
        assert!(cache.get("acme-prod").await.unwrap().is_none());
        register(store.as_ref(), "acme-prod", "first");
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(
            cache.get("acme-prod").await.unwrap().is_some(),
            "a project is reachable seconds after it is created, not a minute after"
        );
    }

    #[tokio::test]
    async fn an_entry_is_read_again_once_its_ttl_is_out() {
        let (_d, store) = store();
        register(store.as_ref(), "acme-prod", "first");
        let cache = Registry::new(store.clone())
            .with_ttl(Duration::from_millis(1), Duration::from_millis(1));
        assert!(cache.get("acme-prod").await.unwrap().is_some());
        registry::delete(store.as_ref(), "acme-prod").expect("it unregisters");
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(
            cache.get("acme-prod").await.unwrap().is_none(),
            "a deleted project stops answering"
        );
    }

    #[tokio::test]
    async fn the_cache_stays_inside_its_capacity() {
        let (_d, store) = store();
        let cache = Registry::new(store.clone()).with_capacity(16);
        for n in 0..200 {
            let tenant_ref = format!("t-{n}");
            register(store.as_ref(), &tenant_ref, "s");
            cache.get(&tenant_ref).await.unwrap();
            assert!(cache.len().await <= 17, "at {n}: {}", cache.len().await);
        }
        assert!(
            cache.get("t-199").await.unwrap().is_some(),
            "and what it holds is still readable"
        );
    }
}
