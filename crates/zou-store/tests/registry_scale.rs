//! What a registry costs when it is large.
//!
//! The claim this measures is that a deployment can hold a million
//! registered tenants and that the ones nobody is using cost object
//! storage and nothing else. The part of it that lives here is the
//! registry: one object per tenant, so routing a request is a point GET
//! of one key whatever the fleet size, and a node that has never heard
//! of a tenant learns about it in one round trip rather than by reading
//! a list of everybody.
//!
//! That is an argument about the layout, and an argument is not a
//! number, so this registers a fleet against a real backend and reads
//! the lookup latency back at each decade on the way up. Flat is the
//! answer the layout predicts. Anything else means the common operation
//! is paying for the fleet.
//!
//! It does not run unless it is asked for, because a hundred thousand
//! objects is not something `cargo test` should write anywhere:
//!
//!     ZOU_REGISTRY_SCALE=100000 \
//!     ZOU_S3_TEST_ENDPOINT=http://127.0.0.1:9000 \
//!     ZOU_S3_TEST_BUCKET=zou-test \
//!     ZOU_S3_TEST_ACCESS_KEY=... ZOU_S3_TEST_SECRET_KEY=... \
//!     cargo test --release -p zou-store --features s3 --test registry_scale -- --nocapture
//!
//! Without an endpoint it runs against `ZOU_REGISTRY_SCALE_DIR`, or a
//! temporary directory, which measures a filesystem rather than a store
//! and is still worth having as the control: whatever the two backends
//! disagree about, neither should get slower as the fleet grows.
//!
//! Registering is resumable. A ref that is already there is left alone
//! rather than failing the run, so an interrupted fleet is finished by
//! running it again, and a fleet that is already up is measured without
//! being written twice.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use zou_store::registry::{self, RegistryError, Tenant};
use zou_store::{CasStore, LocalFsStore};

/// The ref of the nth tenant. Zero padded so the keys sort the way the
/// numbers do, which makes a partial run easy to read in a bucket
/// listing, and it is a hostname label because every ref is one.
fn tenant_ref(n: usize) -> String {
    format!("t{n:07}")
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// The backend the environment asks for: an S3 endpoint if one is
/// named, a directory otherwise. The directory case keeps the temporary
/// directory alive by handing it back, since dropping it deletes the
/// fleet.
fn backend() -> (Arc<dyn CasStore>, String, Option<tempfile::TempDir>) {
    #[cfg(feature = "s3")]
    if let Ok(endpoint) = std::env::var("ZOU_S3_TEST_ENDPOINT") {
        let var = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set"));
        let store = zou_store::S3Store::new(zou_store::S3Config {
            endpoint: endpoint.clone(),
            region: std::env::var("ZOU_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into()),
            bucket: var("ZOU_S3_TEST_BUCKET"),
            access_key: var("ZOU_S3_TEST_ACCESS_KEY"),
            secret_key: var("ZOU_S3_TEST_SECRET_KEY"),
            session: std::env::var("ZOU_S3_TEST_SESSION_TOKEN")
                .ok()
                .filter(|v| !v.is_empty()),
            dialect: match std::env::var("ZOU_S3_TEST_DIALECT").as_deref() {
                Ok("gcs") => zou_store::Dialect::Gcs,
                _ => zou_store::Dialect::S3,
            },
        });
        // A prefix of its own, named rather than random, because a run
        // that stops halfway has to be finishable by the next one.
        let prefix = std::env::var("ZOU_REGISTRY_SCALE_PREFIX")
            .unwrap_or_else(|_| "registry-scale".to_string());
        let store = zou_store::PrefixStore::new(Box::new(store), &prefix);
        return (Arc::new(store), format!("{endpoint} under {prefix}/"), None);
    }
    match std::env::var("ZOU_REGISTRY_SCALE_DIR") {
        Ok(dir) => (Arc::new(LocalFsStore::new(&dir)), dir, None),
        Err(_) => {
            let dir = tempfile::tempdir().expect("a temporary directory");
            let at = dir.path().display().to_string();
            (Arc::new(LocalFsStore::new(dir.path())), at, Some(dir))
        }
    }
}

/// Latencies, as the handful of numbers worth printing. Sorting is the
/// caller's, so the quantiles are read out of the sample rather than
/// estimated.
struct Spread {
    n: usize,
    p50: Duration,
    p90: Duration,
    p99: Duration,
    max: Duration,
}

impl Spread {
    fn of(mut took: Vec<Duration>) -> Spread {
        assert!(!took.is_empty(), "a spread of nothing is not a measurement");
        took.sort_unstable();
        let at = |q: f64| took[((took.len() as f64 * q) as usize).min(took.len() - 1)];
        Spread {
            n: took.len(),
            p50: at(0.50),
            p90: at(0.90),
            p99: at(0.99),
            max: *took.last().expect("checked above"),
        }
    }
}

impl std::fmt::Display for Spread {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "n={} p50={:.3?} p90={:.3?} p99={:.3?} max={:.3?}",
            self.n, self.p50, self.p90, self.p99, self.max
        )
    }
}

/// Register `from..to`, `jobs` at a time, and give back how long each
/// write took. A ref that is already registered is not an error: this
/// is how an interrupted run finishes.
fn register(
    store: &Arc<dyn CasStore>,
    from: usize,
    to: usize,
    jobs: usize,
) -> (Vec<Duration>, usize) {
    let already = Arc::new(AtomicUsize::new(0));
    let mut written = Vec::new();
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for job in 0..jobs {
            let store = Arc::clone(store);
            let already = Arc::clone(&already);
            handles.push(scope.spawn(move || {
                let mut took = Vec::new();
                for n in (from + job..to).step_by(jobs) {
                    // The secret is a real one in shape and fake in
                    // strength, since nothing here verifies a token and
                    // a hundred thousand calls to the os rng would be
                    // measuring the os rng.
                    let entry = Tenant::new(&tenant_ref(n), &format!("{n:064x}"), n as u64);
                    let started = Instant::now();
                    let answer = registry::create(&*store, &entry);
                    let elapsed = started.elapsed();
                    match answer {
                        Ok(()) => took.push(elapsed),
                        Err(RegistryError::Exists { .. }) => {
                            already.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => panic!("registering {}: {e}", tenant_ref(n)),
                    }
                }
                took
            }));
        }
        for handle in handles {
            written.extend(handle.join().expect("a registering thread"));
        }
    });
    (written, already.load(Ordering::Relaxed))
}

/// Look up `samples` refs spread over a fleet of `size`, the way
/// routing does: one ref, one point read, nothing cached in front of
/// it.
///
/// The refs are picked by a small deterministic generator rather than
/// by walking in order, because reading keys in the order they were
/// written is the one access pattern a backend is most likely to have
/// made cheap by accident.
fn lookups(store: &Arc<dyn CasStore>, size: usize, samples: usize) -> Spread {
    let mut seed = 0x2545f4914f6cdd1du64;
    let mut took = Vec::with_capacity(samples);
    for _ in 0..samples {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let wanted = tenant_ref(seed as usize % size);
        let started = Instant::now();
        let found = registry::get(&**store, &wanted).expect("a lookup answers");
        took.push(started.elapsed());
        assert_eq!(
            found.map(|t| t.tenant_ref),
            Some(wanted.clone()),
            "{wanted} was registered and has to be found"
        );
    }
    Spread::of(took)
}

/// A ref nobody registered, which is what an unauthenticated request
/// naming a project that does not exist costs, and it must cost the
/// same one round trip as a hit rather than a search.
fn misses(store: &Arc<dyn CasStore>, samples: usize) -> Spread {
    let mut took = Vec::with_capacity(samples);
    for n in 0..samples {
        let wanted = format!("nobody-{n:07}");
        let started = Instant::now();
        let found = registry::get(&**store, &wanted).expect("a lookup answers");
        took.push(started.elapsed());
        assert_eq!(found, None);
    }
    Spread::of(took)
}

#[test]
fn a_large_registry_stays_a_point_lookup() {
    let Some(total) = std::env::var("ZOU_REGISTRY_SCALE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
    else {
        eprintln!("skipping: ZOU_REGISTRY_SCALE not set");
        return;
    };
    let jobs = env_usize("ZOU_REGISTRY_SCALE_JOBS", 32);
    let samples = env_usize("ZOU_REGISTRY_SCALE_SAMPLES", 300);
    let (store, what, _keep) = backend();
    println!("registry scale: {total} tenants on {what}, {jobs} jobs");

    // Up in decades, measuring at each one. A fleet ten times the size
    // that answers a lookup in the same time is the whole claim, and it
    // is only visible from more than one point.
    let mut milestone = 1_000.min(total);
    let mut done = 0;
    while done < total {
        let upto = milestone.min(total);
        let started = Instant::now();
        let (written, already) = register(&store, done, upto, jobs);
        let wall = started.elapsed();
        if !written.is_empty() {
            let rate = written.len() as f64 / wall.as_secs_f64();
            println!(
                "  registered {} to {upto}: {} new, {already} already there, {wall:.1?}, {rate:.0}/s, {}",
                done,
                written.len(),
                Spread::of(written)
            );
        } else {
            println!("  registered {done} to {upto}: all {already} already there");
        }
        println!("  lookup at {upto}: {}", lookups(&store, upto, samples));
        done = upto;
        milestone = milestone.saturating_mul(10);
    }

    println!(
        "  lookup of a ref nobody registered: {}",
        misses(&store, 100)
    );

    // Listing is the admin command, and it is the one operation that
    // does read every entry. It is measured so nobody has to guess what
    // `zou tenant list` does to a fleet this size.
    let started = Instant::now();
    let listed = registry::list(&*store).expect("the registry lists");
    println!(
        "  list of {} entries: {:.1?}",
        listed.len(),
        started.elapsed()
    );
    assert!(
        listed.len() >= total,
        "listing found {} of {total} registered",
        listed.len()
    );

    // A custom hostname is a second point read of a second key rather
    // than a search for a needle, so it should cost what a ref lookup
    // costs whatever the fleet size.
    let host = "scale.example.test";
    match registry::add_host(&*store, &tenant_ref(0), host) {
        Ok(()) | Err(RegistryError::HostElsewhere { .. }) => {}
        Err(e) => panic!("claiming a host: {e}"),
    }
    let mut took = Vec::new();
    for _ in 0..100 {
        let started = Instant::now();
        let found = registry::host_ref(&*store, host).expect("a host lookup answers");
        took.push(started.elapsed());
        assert_eq!(found.as_deref(), Some(tenant_ref(0).as_str()));
    }
    println!("  lookup by custom hostname: {}", Spread::of(took));
}
