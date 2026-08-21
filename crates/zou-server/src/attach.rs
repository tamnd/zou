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
//! Neither budget takes a tenant somebody is in the middle of using. A
//! caller that is about to do work holds the tenant while it does it,
//! and a held tenant is passed over by the ceiling and by the sweep. On
//! a node whose working set is up against its ceiling that means the
//! ceiling is briefly exceeded, which is the honest trade: the other
//! answer is stopping a database under work the node has already
//! accepted, and the client sees that as the connection being cut.
//!
//! What starts a database is not in here. This owns the policy, which
//! is a map, two budgets and a clock, and takes the machinery as a
//! [`Backend`] so that the policy is testable without a postmaster and
//! so that an embedded build can bring a tenant up its own way.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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

/// One tenant, up.
///
/// The router is what the http door serves and the dsn is what the pg
/// wire door proxies to, and they are one thing because they are one
/// attach: whichever door is asked first pays for the postmaster and
/// the other one finds it already running.
struct Up {
    router: Router,
    pg: Option<String>,
}

struct Slot {
    /// Built once. A failed attach leaves this empty, so the next
    /// request tries again rather than inheriting the failure.
    up: OnceCell<Arc<Up>>,
    /// Milliseconds since the manager was made, so that touching a
    /// tenant on the read path is one relaxed store and not a lock.
    used: AtomicU64,
    /// How many callers are inside this tenant right now. Not zero and
    /// neither budget will take it, which is what keeps a database from
    /// being stopped under a request that is still talking to it.
    busy: AtomicUsize,
}

impl Slot {
    /// Nobody is inside it, so a budget may take it.
    fn free(&self) -> bool {
        self.busy.load(Ordering::Acquire) == 0
    }
}

/// A tenant held up for as long as this lives.
///
/// What a caller does with the router or the dsn outlives the lookup
/// that produced them, so the lookup hands back the thing that keeps
/// the tenant attached rather than a bare copy of either. Dropping it
/// is what says the work is over, so a holder wants to live as long as
/// the work does: a request until its answer is written, a postgres
/// session until the client goes away.
pub struct Hold {
    _busy: Busy,
    up: Arc<Up>,
}

impl Hold {
    /// The http door for this tenant. A clone, because a router is
    /// cheap to clone and axum wants it by value.
    pub fn router(&self) -> Router {
        self.up.router.clone()
    }

    /// Where a postgres client is proxied to. None is a tenant its
    /// backend brought up without a database to talk to.
    pub fn dsn(&self) -> Option<&str> {
        self.up.pg.as_deref()
    }
}

/// The counting half of [`Hold`], taken before the attach so that a
/// tenant being built is as safe from eviction as one being used, and
/// so that an attach that failed lets go on the way out.
struct Busy(Arc<Slot>);

impl Busy {
    fn of(slot: Arc<Slot>) -> Busy {
        slot.busy.fetch_add(1, Ordering::AcqRel);
        Busy(slot)
    }
}

impl Drop for Busy {
    fn drop(&mut self) {
        self.0.busy.fetch_sub(1, Ordering::Release);
    }
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
    /// The caller holds the answer for as long as it is using it, and
    /// while it does, neither budget will take the tenant away. That is
    /// what the hold is for: a router is a clone and survives a detach,
    /// but the database behind it does not survive the backend stopping
    /// the postmaster, and the client sees that as its connection being
    /// cut halfway through an answer.
    pub async fn hold(&self, entry: &Tenant) -> Result<Hold, String> {
        let slot = self.slot(&entry.tenant_ref).await;
        slot.used.store(self.now(), Ordering::Relaxed);
        // Before the attach, not after: a tenant that is being built is
        // in the map already, and the request paying for the build is
        // the last one that should find it evicted when it gets there.
        let busy = Busy::of(slot.clone());
        let up = self.up(&slot, entry).await?;
        Ok(Hold { _busy: busy, up })
    }

    /// The router on its own, for a caller with no work to hold it
    /// through.
    ///
    /// The hold is over by the time this returns, so what is left
    /// behind is an attach rather than a promise: it is the right thing
    /// for bringing a tenant up ahead of the requests that will use it,
    /// and the wrong thing for serving one out of.
    pub async fn router(&self, entry: &Tenant) -> Result<Router, String> {
        Ok(self.hold(entry).await?.router())
    }

    /// The dsn of the tenant's database, attaching it if it is not up.
    ///
    /// This is the pg wire door's half of the same attach. None is a
    /// tenant whose backend brought it up without a database to talk
    /// to, which is a legitimate answer on the http side and nothing a
    /// postgres client can be given.
    pub async fn dsn(&self, entry: &Tenant) -> Result<Option<String>, String> {
        Ok(self.hold(entry).await?.dsn().map(str::to_string))
    }

    async fn up(&self, slot: &Arc<Slot>, entry: &Tenant) -> Result<Arc<Up>, String> {
        let backend = self.backend.clone();
        let entry = entry.clone();
        let tenant_ref = entry.tenant_ref.clone();
        let up = slot
            .up
            .get_or_try_init(|| async move {
                // Inside the cell, so what is timed is a cold attach
                // and a warm request counts nothing at all. It is also
                // the span worth having: the request that pays for a
                // cold start is slow for a reason no other span shows.
                let start = Instant::now();
                let mut span = crate::ops::span("attach");
                if let Some(span) = span.as_mut() {
                    span.text("zou.tenant", tenant_ref);
                }
                let built = match tokio::task::spawn_blocking(move || backend.up(&entry)).await {
                    Ok(Ok(cfg)) => {
                        let pg = cfg.pg.clone();
                        crate::router(cfg).map(|router| Arc::new(Up { router, pg }))
                    }
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(format!("attach: {e}")),
                };
                crate::ops::attach(built.is_ok(), start);
                if let Some(mut span) = span {
                    if let Err(e) = &built {
                        span.failed(e.clone());
                    }
                    crate::ops::record(span);
                }
                built
            })
            .await?;
        // After, not before: a cold attach that pushed the node over
        // its ceiling should evict something older than the thing it
        // just built, and evicting before would let the ceiling be
        // exceeded by exactly the tenant that is about to be used.
        self.enforce().await;
        Ok(up.clone())
    }

    /// Drop everything untouched for longer than the idle budget. A
    /// server calls this on a timer, since a node that has gone quiet
    /// is exactly the one that should be letting go of leases and is
    /// also the one with no requests to notice on.
    pub async fn sweep(&self) {
        let idle = match u64::try_from(self.idle.as_nanos()) {
            Ok(ns) => ns,
            Err(_) => return,
        };
        let cutoff = self.now().saturating_sub(idle);
        let mut slots = self.slots.lock().await;
        // A held tenant is untouched only in the sense that nothing has
        // attached it lately, and a postgres session that has sat quiet
        // for an hour is exactly that. It is still somebody's, so the
        // idle budget passes over it the same way the ceiling does.
        let stale: Vec<String> = slots
            .iter()
            .filter(|(_, slot)| slot.used.load(Ordering::Relaxed) < cutoff && slot.free())
            .map(|(tenant_ref, _)| tenant_ref.clone())
            .collect();
        for tenant_ref in stale {
            slots.remove(&tenant_ref);
            self.backend.down(&tenant_ref);
        }
        crate::ops::attached(slots.len());
        drop(slots);
        // The ceiling too, because the attach that last exceeded it may
        // have been held over it and there is no promise of another one
        // arriving to try again.
        self.enforce().await;
    }

    /// Let one tenant go now, for a project that was just deleted or a
    /// secret that was just rotated.
    pub async fn detach(&self, tenant_ref: &str) {
        let mut slots = self.slots.lock().await;
        slots.remove(tenant_ref);
        crate::ops::attached(slots.len());
        drop(slots);
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
                    up: OnceCell::new(),
                    used: AtomicU64::new(0),
                    busy: AtomicUsize::new(0),
                })
            })
            .clone()
    }

    /// Back down to the ceiling, oldest use first, skipping whatever
    /// somebody is inside.
    ///
    /// Skipping is why this can finish with the node still over its
    /// ceiling. It happens when the working set is the ceiling and every
    /// tenant on the node is being used at once, and it lasts until the
    /// holds are let go, at which point the next attach or the next
    /// sweep takes the room back.
    async fn enforce(&self) {
        let mut slots = self.slots.lock().await;
        // Reported from here because this runs on every attach and
        // already holds the lock the count would need.
        crate::ops::attached(slots.len());
        if slots.len() <= self.max {
            return;
        }
        let mut by_use: Vec<(u64, String)> = slots
            .iter()
            .filter(|(_, slot)| slot.free())
            .map(|(tenant_ref, slot)| (slot.used.load(Ordering::Relaxed), tenant_ref.clone()))
            .collect();
        by_use.sort_unstable();
        let over = slots.len() - self.max;
        for (_, tenant_ref) in by_use.into_iter().take(over) {
            // Read again rather than trusted from the filter: a request
            // takes its tenant without this lock, so the last word on
            // whether one is in use is the word said next to the
            // removal itself.
            if slots.get(&tenant_ref).is_some_and(|slot| !slot.free()) {
                continue;
            }
            slots.remove(&tenant_ref);
            self.backend.down(&tenant_ref);
        }
        let left = slots.len().saturating_sub(self.max);
        if left > 0 {
            log::debug!(
                "{left} tenants over the ceiling of {}, all in use",
                self.max
            );
        }
        crate::ops::attached(slots.len());
    }

    /// Nanoseconds, not milliseconds: eviction sorts on this, and at
    /// millisecond resolution a burst of attaches all land on one tick,
    /// leaving the sort to break the tie by tenant name instead of by
    /// use. Nanoseconds keep every touch distinct.
    fn now(&self) -> u64 {
        u64::try_from(self.born.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    /// A backend that starts nothing. A dsn is only parsed here, never
    /// dialled, so a tenant can be up in a test without a postmaster
    /// behind it.
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
                pg: Some(format!(
                    "host=127.0.0.1 port=5432 user=zou dbname={}",
                    entry.tenant_ref
                )),
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
    async fn both_doors_share_one_attach() {
        let (fake, attached) = manager();
        let _ = attached.router(&entry("acme-prod")).await.unwrap();
        let dsn = attached.dsn(&entry("acme-prod")).await.unwrap();
        assert_eq!(
            dsn.as_deref(),
            Some("host=127.0.0.1 port=5432 user=zou dbname=acme-prod")
        );
        assert_eq!(
            fake.ups(),
            vec!["acme-prod"],
            "a postgres client arriving after a request does not start a second postmaster"
        );
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
    async fn a_tenant_somebody_is_using_is_not_the_one_the_ceiling_takes() {
        let (fake, attached) = manager();
        let attached = attached.with_budget(2, IDLE);
        // The oldest use by a mile, and in the middle of a request, so
        // the ceiling has to reach past it for the next one instead.
        let one = attached.hold(&entry("one")).await.unwrap();
        let _ = attached.router(&entry("two")).await.unwrap();
        let _ = attached.router(&entry("three")).await.unwrap();
        assert_eq!(
            fake.downs(),
            vec!["two"],
            "the least recently used that nobody was inside"
        );
        drop(one);
        // And once the request is over it is an ordinary candidate.
        let _ = attached.router(&entry("four")).await.unwrap();
        assert_eq!(fake.downs(), vec!["two", "one"]);
    }

    #[tokio::test]
    async fn the_ceiling_is_exceeded_rather_than_a_request_failed() {
        let (fake, attached) = manager();
        let attached = attached.with_budget(1, IDLE);
        let held: Vec<Hold> = vec![
            attached.hold(&entry("one")).await.unwrap(),
            attached.hold(&entry("two")).await.unwrap(),
            attached.hold(&entry("three")).await.unwrap(),
        ];
        assert_eq!(attached.len().await, 3, "a ceiling of one, three in use");
        assert!(
            fake.downs().is_empty(),
            "nothing was stopped under work the node had accepted"
        );
        drop(held);
        // Nothing else attaches, so it is the sweep that takes the room
        // back rather than a request that happened to arrive.
        attached.sweep().await;
        assert_eq!(attached.len().await, 1);
        assert_eq!(fake.downs(), vec!["one", "two"]);
    }

    #[tokio::test]
    async fn an_idle_tenant_somebody_is_still_connected_to_stays() {
        let (fake, attached) = manager();
        let attached = attached.with_budget(MAX_ATTACHED, Duration::from_millis(20));
        let session = attached.hold(&entry("quiet")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        attached.sweep().await;
        assert!(
            fake.downs().is_empty(),
            "a session that has sat quiet is not a project nobody is using"
        );
        drop(session);
        attached.sweep().await;
        assert_eq!(fake.downs(), vec!["quiet"]);
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
