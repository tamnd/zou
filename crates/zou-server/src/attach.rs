//! Which tenants are up, and which ones stop being up.
//!
//! Resolution says a request is for `acme-prod`. Something then has to
//! turn that ref into a database that answers: acquire the lease, get
//! the manifest, start postgres, build the router in front of it. That
//! is expensive, it is the reason NFR-12 has a number in it, and it is
//! why it happens once and then the result is kept.
//!
//! Kept, not kept forever. A node with a thousand tenants on it has a
//! thousand of everything, so this holds two budgets: a ceiling on how
//! many may be up at once, and a patience for how long an untouched one
//! stays up. Both evict the least recently used, because the tenant
//! nobody has asked for in an hour is the cheapest one to make somebody
//! wait for again.
//!
//! What starts a database is not in here. This owns the policy, which
//! is a map, two budgets and a clock, and takes the machinery as a
//! [`Backend`] so that the policy is testable without a postmaster and
//! so that an embedded build can bring a tenant up its own way.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use tokio::sync::{Mutex, OnceCell};
use zou_store::registry::Tenant;

use crate::Config;

/// What it takes to bring a tenant up and let it go again.
///
/// `up` is called at most once per attach, on a blocking thread,
/// because it starts a database. It answers with the config the
/// tenant's front door is built from, so a backend decides the dsn, the
/// store target and the per tenant caps, and this module decides
/// nothing about any of them.
pub trait Backend: Send + Sync {
    fn up(&self, entry: &Tenant) -> Result<Config, String>;
    /// Called after a tenant has been dropped from the map. It must be
    /// safe to call for a ref that was never up, since a failed attach
    /// leaves nothing behind and is swept the same way.
    fn down(&self, tenant_ref: &str);
}

/// The default ceiling on tenants up at once. NFR-13 asks for a
/// thousand small ones on an eight vCPU node, so the budget is not the
/// thing that stops it being met.
pub const MAX_ATTACHED: usize = 1024;

/// How long a tenant nobody has asked for stays up. Long enough that a
/// person clicking around a dashboard never waits twice, short enough
/// that an abandoned project is not still holding a lease at midnight.
pub const IDLE: Duration = Duration::from_secs(15 * 60);

struct Slot {
    /// Built once. A failed attach leaves this empty, so the next
    /// request tries again rather than inheriting the failure.
    router: OnceCell<Router>,
    /// Milliseconds since the manager was made, so that touching a
    /// tenant on the read path is one relaxed store and not a lock.
    used: AtomicU64,
}

/// The attached set.
pub struct Attached {
    backend: Arc<dyn Backend>,
    max: usize,
    idle: Duration,
    born: Instant,
    /// Held only while the map is read or written, never across an
    /// attach: the attach itself is serialised per ref by that ref's
    /// own cell, so two requests for one cold tenant start one
    /// postmaster while two requests for two cold tenants do not wait
    /// on each other.
    slots: Mutex<HashMap<String, Arc<Slot>>>,
}

impl Attached {
    pub fn new(backend: Arc<dyn Backend>) -> Attached {
        Attached {
            backend,
            max: MAX_ATTACHED,
            idle: IDLE,
            born: Instant::now(),
            slots: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_budget(mut self, max: usize, idle: Duration) -> Attached {
        self.max = max.max(1);
        self.idle = idle;
        self
    }

    /// The front door for a tenant, attaching it if it is not up.
    ///
    /// The returned router is a clone, so a request that is already
    /// running holds what it needs even if the tenant is detached
    /// underneath it. What that does not save it from is the backend
    /// stopping the database it talks to, which is why detach picks the
    /// least recently used: a request in flight has just touched its
    /// tenant, so it is the last thing eviction reaches for.
    pub async fn router(&self, entry: &Tenant) -> Result<Router, String> {
        let slot = self.slot(&entry.tenant_ref).await;
        slot.used.store(self.now(), Ordering::Relaxed);
        let backend = self.backend.clone();
        let entry = entry.clone();
        let router = slot
            .router
            .get_or_try_init(|| async move {
                let cfg = tokio::task::spawn_blocking(move || backend.up(&entry))
                    .await
                    .map_err(|e| format!("attach: {e}"))??;
                crate::router(cfg)
            })
            .await?;
        // After, not before: a cold attach that pushed the node over
        // its ceiling should evict something older than the thing it
        // just built, and evicting before would let the ceiling be
        // exceeded by exactly the tenant that is about to be used.
        self.enforce().await;
        Ok(router.clone())
    }

    /// Drop everything untouched for longer than the idle budget. A
    /// server calls this on a timer, since a node that has gone quiet
    /// is exactly the one that should be letting go of leases and is
    /// also the one with no requests to notice on.
    pub async fn sweep(&self) {
        let idle = match u64::try_from(self.idle.as_millis()) {
            Ok(ms) => ms,
            Err(_) => return,
        };
        let cutoff = self.now().saturating_sub(idle);
        let mut slots = self.slots.lock().await;
        let stale: Vec<String> = slots
            .iter()
            .filter(|(_, slot)| slot.used.load(Ordering::Relaxed) < cutoff)
            .map(|(tenant_ref, _)| tenant_ref.clone())
            .collect();
        for tenant_ref in stale {
            slots.remove(&tenant_ref);
            self.backend.down(&tenant_ref);
        }
    }

    /// Let one tenant go now, for a project that was just deleted or a
    /// secret that was just rotated.
    pub async fn detach(&self, tenant_ref: &str) {
        self.slots.lock().await.remove(tenant_ref);
        self.backend.down(tenant_ref);
    }

    /// How many are up, which is what the density gate is measured in.
    pub async fn len(&self) -> usize {
        self.slots.lock().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    async fn slot(&self, tenant_ref: &str) -> Arc<Slot> {
        let mut slots = self.slots.lock().await;
        slots
            .entry(tenant_ref.to_string())
            .or_insert_with(|| {
                Arc::new(Slot {
                    router: OnceCell::new(),
                    used: AtomicU64::new(0),
                })
            })
            .clone()
    }

    /// Back down to the ceiling, oldest use first.
    async fn enforce(&self) {
        let mut slots = self.slots.lock().await;
        if slots.len() <= self.max {
            return;
        }
        let mut by_use: Vec<(u64, String)> = slots
            .iter()
            .map(|(tenant_ref, slot)| (slot.used.load(Ordering::Relaxed), tenant_ref.clone()))
            .collect();
        by_use.sort_unstable();
        let over = slots.len() - self.max;
        for (_, tenant_ref) in by_use.into_iter().take(over) {
            slots.remove(&tenant_ref);
            self.backend.down(&tenant_ref);
        }
    }

    fn now(&self) -> u64 {
        u64::try_from(self.born.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    /// A backend that starts nothing. `Config::pg` of None is a router
    /// with no pool behind it, which is enough to prove a tenant is up
    /// without a postmaster in the test.
    #[derive(Default)]
    struct Fake {
        ups: StdMutex<Vec<String>>,
        downs: StdMutex<Vec<String>>,
        fails: StdMutex<Vec<String>>,
    }

    impl Fake {
        fn fail(&self, tenant_ref: &str) {
            self.fails.lock().unwrap().push(tenant_ref.to_string());
        }
        fn stop_failing(&self) {
            self.fails.lock().unwrap().clear();
        }
        fn ups(&self) -> Vec<String> {
            self.ups.lock().unwrap().clone()
        }
        fn downs(&self) -> Vec<String> {
            self.downs.lock().unwrap().clone()
        }
    }

    impl Backend for Fake {
        fn up(&self, entry: &Tenant) -> Result<Config, String> {
            if self.fails.lock().unwrap().contains(&entry.tenant_ref) {
                return Err(format!("{} will not start", entry.tenant_ref));
            }
            self.ups.lock().unwrap().push(entry.tenant_ref.clone());
            Ok(Config {
                jwt_secret: entry.jwt_secret.as_bytes().to_vec(),
                ..Config::default()
            })
        }
        fn down(&self, tenant_ref: &str) {
            self.downs.lock().unwrap().push(tenant_ref.to_string());
        }
    }

    fn entry(tenant_ref: &str) -> Tenant {
        Tenant::new(
            tenant_ref,
            "super-secret-jwt-token-with-at-least-32-characters-long",
            1,
        )
    }

    fn manager() -> (Arc<Fake>, Attached) {
        let backend = Arc::new(Fake::default());
        (backend.clone(), Attached::new(backend))
    }

    #[tokio::test]
    async fn a_tenant_is_brought_up_once_and_then_reused() {
        let (fake, attached) = manager();
        for _ in 0..5 {
            let _ = attached.router(&entry("acme-prod")).await.unwrap();
        }
        assert_eq!(fake.ups(), vec!["acme-prod"], "one attach, five requests");
        assert_eq!(attached.len().await, 1);
    }

    #[tokio::test]
    async fn two_requests_for_one_cold_tenant_start_one_database() {
        let (fake, attached) = manager();
        let attached = Arc::new(attached);
        let mut waiting = Vec::new();
        for _ in 0..8 {
            let attached = attached.clone();
            waiting.push(tokio::spawn(async move {
                let _ = attached.router(&entry("acme-prod")).await.unwrap();
            }));
        }
        for one in waiting {
            one.await.unwrap();
        }
        assert_eq!(
            fake.ups(),
            vec!["acme-prod"],
            "or the ceiling counts one tenant twice and two postmasters own one lease"
        );
    }

    #[tokio::test]
    async fn an_attach_that_failed_is_tried_again_rather_than_remembered() {
        let (fake, attached) = manager();
        fake.fail("acme-prod");
        assert!(attached.router(&entry("acme-prod")).await.is_err());
        fake.stop_failing();
        assert!(
            attached.router(&entry("acme-prod")).await.is_ok(),
            "a store that was briefly unreachable is not a project that is down forever"
        );
    }

    #[tokio::test]
    async fn the_ceiling_holds_and_takes_the_least_recently_used() {
        let (fake, attached) = manager();
        let attached = attached.with_budget(2, IDLE);
        let _ = attached.router(&entry("one")).await.unwrap();
        let _ = attached.router(&entry("two")).await.unwrap();
        // Touched, so it is not the oldest use any more.
        let _ = attached.router(&entry("one")).await.unwrap();
        let _ = attached.router(&entry("three")).await.unwrap();
        assert_eq!(attached.len().await, 2);
        assert_eq!(fake.downs(), vec!["two"]);
        let _ = attached.router(&entry("two")).await.unwrap();
        assert_eq!(
            fake.ups(),
            vec!["one", "two", "three", "two"],
            "and the evicted one comes back by being asked for"
        );
    }

    #[tokio::test]
    async fn an_idle_tenant_is_let_go_and_a_busy_one_is_not() {
        let (fake, attached) = manager();
        let attached = attached.with_budget(MAX_ATTACHED, Duration::from_millis(20));
        let _ = attached.router(&entry("quiet")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        let _ = attached.router(&entry("busy")).await.unwrap();
        attached.sweep().await;
        assert_eq!(fake.downs(), vec!["quiet"]);
        assert_eq!(attached.len().await, 1);
    }

    #[tokio::test]
    async fn nothing_is_let_go_before_its_time() {
        let (fake, attached) = manager();
        let attached = attached.with_budget(MAX_ATTACHED, Duration::from_secs(600));
        let _ = attached.router(&entry("acme-prod")).await.unwrap();
        attached.sweep().await;
        assert!(fake.downs().is_empty());
        assert_eq!(attached.len().await, 1);
    }

    #[tokio::test]
    async fn a_tenant_can_be_dropped_on_purpose() {
        let (fake, attached) = manager();
        let _ = attached.router(&entry("acme-prod")).await.unwrap();
        attached.detach("acme-prod").await;
        assert!(attached.is_empty().await);
        assert_eq!(fake.downs(), vec!["acme-prod"]);
        let _ = attached.router(&entry("acme-prod")).await.unwrap();
        assert_eq!(
            fake.ups(),
            vec!["acme-prod", "acme-prod"],
            "a rotated secret is a fresh attach, not a stale router"
        );
    }
}
