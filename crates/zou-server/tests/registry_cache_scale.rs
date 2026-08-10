//! What a node costs when the registry in front of it is large.
//!
//! The store side of that question is measured in zou-store's
//! `registry_scale` test, which says a lookup is a point read whatever
//! the fleet size. This is the other half: a node holds a bounded cache
//! of entries, so what a hundred thousand registered tenants cost the
//! process is the bound and not the fleet, and a tenant nobody has
//! asked for costs nothing at all.
//!
//! It runs against the fleet that test registered, and skips unless it
//! is pointed at one:
//!
//!     ZOU_REGISTRY_SCALE=100000 \
//!     ZOU_REGISTRY_SCALE_TARGET=s3://zou-scale/registry-scale \
//!     ZOU_S3_ENDPOINT=http://127.0.0.1:9010 \
//!     AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
//!     cargo test --release -p zou-server --test registry_cache_scale -- --nocapture

use std::sync::Arc;
use std::time::{Duration, Instant};

use zou_server::tenant::Registry;
use zou_store::{CasStore, open_store};

fn tenant_ref(n: usize) -> String {
    format!("t{n:07}")
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Quantiles read out of the sample rather than estimated.
fn spread(mut took: Vec<Duration>) -> String {
    took.sort_unstable();
    let at = |q: f64| took[((took.len() as f64 * q) as usize).min(took.len() - 1)];
    format!(
        "n={} p50={:.3?} p90={:.3?} p99={:.3?} max={:.3?}",
        took.len(),
        at(0.50),
        at(0.90),
        at(0.99),
        took.last().copied().unwrap_or_default()
    )
}

/// Resident memory of this process, or `None` where it cannot be read
/// without a dependency. Linux is where the fleet numbers are taken, so
/// Linux is what this reads.
fn resident() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
        let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
        return Some(pages * 4096);
    }
    #[cfg(not(target_os = "linux"))]
    None
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// A deterministic walk over the fleet, so the run is repeatable and
/// nothing reads keys in the order they were written.
fn refs(count: usize, size: usize) -> Vec<String> {
    let mut seed = 0x9e3779b97f4a7c15u64;
    (0..count)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            tenant_ref(seed as usize % size)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_holds_a_bound_rather_than_a_fleet() {
    let Some(size) = std::env::var("ZOU_REGISTRY_SCALE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
    else {
        eprintln!("skipping: ZOU_REGISTRY_SCALE not set");
        return;
    };
    let Ok(target) = std::env::var("ZOU_REGISTRY_SCALE_TARGET") else {
        eprintln!("skipping: ZOU_REGISTRY_SCALE_TARGET not set");
        return;
    };
    let store: Arc<dyn CasStore> = Arc::from(open_store(&target).expect("the fleet's store opens"));
    let registry = Registry::new(Arc::clone(&store));
    println!("registry cache: {size} tenants on {target}");

    let base = resident();

    // Cold, which is what a node that has just come up sees: every ref
    // is a round trip, and the fleet is large enough that the cache is
    // evicting the whole time.
    let cold = env_usize("ZOU_REGISTRY_SCALE_COLD", 2_000);
    let mut took = Vec::with_capacity(cold);
    for wanted in refs(cold, size) {
        let started = Instant::now();
        let found = registry.get(&wanted).await.expect("a lookup answers");
        took.push(started.elapsed());
        assert_eq!(
            found.map(|t| t.tenant_ref).as_deref(),
            Some(wanted.as_str())
        );
    }
    println!("  cold, one round trip each: {}", spread(took));

    // Warm, which is what a node serving traffic sees: a working set
    // much smaller than the fleet, asked for over and over.
    let working = env_usize("ZOU_REGISTRY_SCALE_WORKING", 500);
    let set = refs(working, size);
    for wanted in &set {
        registry.get(wanted).await.expect("a lookup answers");
    }
    let mut took = Vec::with_capacity(working * 20);
    for _ in 0..20 {
        for wanted in &set {
            let started = Instant::now();
            registry.get(wanted).await.expect("a lookup answers");
            took.push(started.elapsed());
        }
    }
    println!("  warm, a working set of {working}: {}", spread(took));

    // The thing the bound is for: ask for far more distinct tenants
    // than the cache holds and watch the process not grow with the
    // fleet.
    let churn = env_usize("ZOU_REGISTRY_SCALE_CHURN", 20_000);
    let started = Instant::now();
    for wanted in refs(churn, size) {
        registry.get(&wanted).await.expect("a lookup answers");
    }
    let wall = started.elapsed();
    println!(
        "  churn over {churn} distinct refs: {wall:.1?}, {:.0}/s",
        churn as f64 / wall.as_secs_f64()
    );

    match (base, resident()) {
        (Some(base), Some(now)) => println!(
            "  resident: {:.1} MiB before, {:.1} MiB after {} distinct tenants seen",
            mib(base),
            mib(now),
            cold + working + churn
        ),
        _ => println!("  resident: not read on this platform"),
    }

    // A ref nobody registered is a miss with a short leash of its own,
    // which is what hostname probing costs a node.
    let mut took = Vec::with_capacity(200);
    for n in 0..200 {
        let wanted = format!("nobody-{n:07}");
        let started = Instant::now();
        let found = registry.get(&wanted).await.expect("a lookup answers");
        took.push(started.elapsed());
        assert_eq!(found, None);
    }
    println!("  a ref nobody registered: {}", spread(took));
}
