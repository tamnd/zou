//! What a failover costs, measured rather than argued about.
//!
//! NFR-41 says RTO is at most the lease TTL plus the attach, and that is
//! a claim about wall clock, so this test spends wall clock. A writer
//! takes a lease and runs a real heartbeat, the heartbeat is dropped,
//! which is what a process death looks like from the store's side since
//! drop stops renewing without releasing anything, and a standby is
//! timed from that moment until it is the writer.
//!
//! Everything here uses short TTLs so a run takes seconds instead of a
//! minute. The shape does not depend on the TTL: recovery is whatever is
//! left of it at the moment of death, plus one CAS. What the numbers
//! confirm is that nothing else is in there.
//!
//! Against a local store by default. With `ZOU_S3_TEST_ENDPOINT` set the
//! same measurement runs against a real object store, because a CAS over
//! the network is the term the local run cannot show.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use zou_store::heartbeat::Heartbeat;
use zou_store::layout::TenantLayout;
use zou_store::lease::{self, LeaseError};
use zou_store::{CasStore, LocalFsStore, Manifest};

/// The TTL under test, seconds. Two is enough for the heartbeat to renew
/// twice on its TTL/3 cadence, so a death lands mid window the way a
/// real one does, and it keeps this file cheap on every CI run.
///
/// `ZOU_FAILOVER_TTL=15` reruns the whole thing at the shipped default,
/// which is the number worth writing down and too slow to spend on every
/// commit.
fn ttl() -> u64 {
    number("ZOU_FAILOVER_TTL", 2)
}

/// How many deaths to time. Three shows a spread, `ZOU_FAILOVER_ROUNDS`
/// buys a distribution.
fn rounds() -> usize {
    number("ZOU_FAILOVER_ROUNDS", 3) as usize
}

fn number(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(fallback)
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock")
        .as_secs()
}

fn tenant(store: &dyn CasStore, name: &str) -> TenantLayout {
    let layout = TenantLayout::new(name);
    store
        .put_if_absent(&layout.manifest(), &Manifest::new(name, 18).to_json())
        .expect("a manifest to lease against");
    layout
}

/// One death, timed: hold the lease with a live heartbeat, kill it, and
/// return how long the standby took to become the writer.
fn one_failover(store: Arc<dyn CasStore>, layout: &TenantLayout) -> Duration {
    let held = lease::acquire_at(
        &*store,
        layout,
        "node-a",
        Some("http://10.0.0.4:8000"),
        ttl(),
        now_unix(),
    )
    .expect("node-a takes the lease");
    let epoch = held.epoch;
    let hb = Heartbeat::spawn(
        Arc::clone(&store),
        layout.clone(),
        Arc::new(Mutex::new(held)),
        ttl(),
    );
    // Long enough to renew, so the lease that has to run out is a
    // renewed one and not the one acquire wrote.
    std::thread::sleep(Duration::from_millis(ttl() * 1000 / 2));
    assert!(!hb.lost(), "the holder lost its lease while it was healthy");

    let died = Instant::now();
    drop(hb);
    let taken = lease::takeover(
        &*store,
        layout,
        "node-b",
        Some("http://10.0.0.5:8000"),
        ttl(),
        Duration::from_secs(ttl() * 4),
    )
    .expect("the standby takes over");
    let rto = died.elapsed();

    assert!(
        taken.epoch > epoch,
        "a takeover has to move the epoch, or nothing is fenced"
    );
    assert_eq!(
        lease::holder(&*store, layout, now_unix())
            .expect("a manifest")
            .expect("a holder")
            .endpoint,
        Some("http://10.0.0.5:8000".to_string()),
        "and the fleet has to be able to find the new writer"
    );
    rto
}

fn report(what: &str, mut times: Vec<Duration>) {
    times.sort();
    let ms = |d: &Duration| d.as_millis();
    println!(
        "failover rto over {} rounds against {what}, ttl {}s: min {} ms, p50 {} ms, max {} ms",
        times.len(),
        ttl(),
        ms(times.first().expect("a round")),
        ms(&times[times.len() / 2]),
        ms(times.last().expect("a round")),
    );
    let worst = *times.last().expect("a round");
    assert!(
        worst <= Duration::from_secs(ttl()) + Duration::from_millis(1500),
        "recovery took {} ms, which is more than the ttl plus a CAS: NFR-41 says the ttl is the bound",
        worst.as_millis(),
    );
}

#[test]
fn a_dead_holder_costs_the_rest_of_its_ttl_and_nothing_else() {
    let dir = tempfile::tempdir().expect("a directory");
    let store: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dir.path()));
    let times = (0..rounds())
        .map(|round| {
            let layout = tenant(&*store, &format!("t{round}"));
            one_failover(Arc::clone(&store), &layout)
        })
        .collect();
    report("a local store", times);
}

#[cfg(feature = "s3")]
#[test]
fn the_same_against_a_real_object_store() {
    let Ok(endpoint) = std::env::var("ZOU_S3_TEST_ENDPOINT") else {
        eprintln!("skipping: ZOU_S3_TEST_ENDPOINT is not set");
        return;
    };
    let var = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set"));
    let store: Arc<dyn CasStore> = Arc::new(zou_store::S3Store::new(zou_store::S3Config {
        endpoint,
        region: std::env::var("ZOU_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into()),
        bucket: var("ZOU_S3_TEST_BUCKET"),
        access_key: var("ZOU_S3_TEST_ACCESS_KEY"),
        secret_key: var("ZOU_S3_TEST_SECRET_KEY"),
        dialect: zou_store::Dialect::S3,
    }));
    let run = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock")
        .as_nanos();
    let times = (0..rounds())
        .map(|round| {
            let layout = tenant(&*store, &format!("failover-{run}-{round}"));
            one_failover(Arc::clone(&store), &layout)
        })
        .collect();
    report("a real object store", times);
}

#[test]
fn a_clean_shutdown_hands_over_with_no_wait_at_all() {
    // Which is the whole reason detach clears the lease: a rolling
    // restart should not cost a TTL per tenant.
    let dir = tempfile::tempdir().expect("a directory");
    let store: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dir.path()));
    let layout = tenant(&*store, "t1");
    let held = lease::acquire(&*store, &layout, "node-a", ttl(), now_unix()).expect("the lease");
    let hb = Heartbeat::spawn(
        Arc::clone(&store),
        layout.clone(),
        Arc::new(Mutex::new(held)),
        ttl(),
    );
    let stopped = Instant::now();
    hb.detach().expect("a clean release");
    lease::takeover(
        &*store,
        &layout,
        "node-b",
        None,
        ttl(),
        Duration::from_secs(ttl() * 4),
    )
    .expect("nobody holds it");
    let rto = stopped.elapsed();
    println!("failover rto after a clean detach: {} ms", rto.as_millis());
    assert!(
        rto < Duration::from_millis(500),
        "a clean handover waited {} ms, which means it waited for something",
        rto.as_millis()
    );
}

#[test]
fn the_node_that_died_cannot_write_again() {
    // RPO is the other half of NFR-41, and it rests on this: the holder
    // that comes back finds itself fenced rather than writing into a
    // tenant somebody else now owns.
    let dir = tempfile::tempdir().expect("a directory");
    let store: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dir.path()));
    let layout = tenant(&*store, "t1");
    let mut zombie =
        lease::acquire(&*store, &layout, "node-a", ttl(), now_unix()).expect("the lease");
    std::thread::sleep(Duration::from_secs(ttl() + 1));
    lease::takeover(
        &*store,
        &layout,
        "node-b",
        None,
        ttl(),
        Duration::from_secs(ttl() * 4),
    )
    .expect("the standby takes over an expired lease");

    let err = lease::renew(&*store, &layout, &mut zombie, ttl(), now_unix()).unwrap_err();
    assert!(
        matches!(err, LeaseError::Lost { ref holder, .. } if holder == "node-b"),
        "a returning holder has to be told it is not the holder: {err}"
    );
}

#[test]
fn two_standbys_racing_produce_one_writer() {
    let dir = tempfile::tempdir().expect("a directory");
    let store: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dir.path()));
    let layout = tenant(&*store, "t1");
    lease::acquire(&*store, &layout, "node-a", ttl(), now_unix()).expect("the lease");

    // Both start while the dead lease still has time on it, and both
    // give up before the winner's own lease could run out, so exactly
    // one of them can come back a writer.
    let racers: Vec<_> = ["node-b", "node-c"]
        .into_iter()
        .map(|node| {
            let store = Arc::clone(&store);
            let layout = layout.clone();
            std::thread::spawn(move || {
                lease::takeover(
                    &*store,
                    &layout,
                    node,
                    None,
                    ttl(),
                    Duration::from_secs(ttl() + 1),
                )
                .map(|held| (node, held.epoch))
            })
        })
        .collect();
    let results: Vec<_> = racers
        .into_iter()
        .map(|r| r.join().expect("a thread"))
        .collect();

    let winners: Vec<_> = results.iter().filter_map(|r| r.as_ref().ok()).collect();
    assert_eq!(
        winners.len(),
        1,
        "exactly one standby may end up the writer, got {results:?}"
    );
    let (winner, _) = winners[0];
    for loser in results.iter().filter_map(|r| r.as_ref().err()) {
        assert!(
            matches!(loser, LeaseError::Held { holder, .. } if holder == winner),
            "the standby that lost has to know who to forward to: {loser}"
        );
    }
}
