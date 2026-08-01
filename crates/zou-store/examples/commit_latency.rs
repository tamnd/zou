//! Commit latency harness: measures append-to-ack latency per tier.
//!
//! Run: cargo run --release --example commit_latency [target]
//!
//! Without a target every tier runs against a fresh temp directory, so
//! the numbers measure the pipeline itself: batching, framing, fsync,
//! rename. With a target the same pipeline runs against whatever the
//! string names, a directory, sqlite://, a .zou file, or s3:// with the
//! s3 feature, under a nonce prefix that is deleted afterwards. Sample
//! counts come from ZOU_BENCH_SAMPLES and ZOU_BENCH_WARMUP for slow
//! remote runs. Recorded baselines live in docs/benchmarks.md.

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use zou_store::layout::TenantLayout;
use zou_store::tier::{BufferedTarget, ExpressTarget, PureS3Target, WalTarget};
use zou_store::{CasStore, GroupCommit, GroupCommitConfig, LocalFsStore, Lsn, open_store};

const RECORD_BYTES: usize = 128;

fn env_count(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

fn bench(
    name: &str,
    store: Arc<dyn CasStore>,
    tenant: &str,
    warmup: usize,
    samples: usize,
    make_target: impl Fn(Arc<dyn CasStore>) -> Arc<dyn WalTarget>,
) {
    let layout = TenantLayout::new(tenant);
    let prefix = layout.prefix().to_string();
    let target = make_target(Arc::clone(&store));
    let gc = GroupCommit::builder(Arc::clone(&store), layout)
        .session(1, 1)
        .start_lsn(Lsn(0))
        .config(GroupCommitConfig::default())
        .target(target)
        .build();

    let record = vec![7u8; RECORD_BYTES];
    for _ in 0..warmup {
        gc.append(&record).unwrap().wait().unwrap();
    }

    let mut micros: Vec<u128> = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        gc.append(&record).unwrap().wait().unwrap();
        micros.push(start.elapsed().as_micros());
    }
    gc.close().unwrap();

    for key in store.list(&prefix).unwrap() {
        store.delete(&key).unwrap();
    }

    micros.sort_unstable();
    println!(
        "{name:>8}  p50 {:>6} us  p95 {:>6} us  p99 {:>6} us  max {:>6} us",
        percentile(&micros, 0.50),
        percentile(&micros, 0.95),
        percentile(&micros, 0.99),
        micros[micros.len() - 1],
    );
}

fn main() {
    let target = std::env::args().nth(1);
    let warmup = env_count("ZOU_BENCH_WARMUP", 200);
    let samples = env_count("ZOU_BENCH_SAMPLES", 2000);

    // Keep the temp dir alive for the whole run when no target is given.
    let tempdir;
    let (store, backend): (Arc<dyn CasStore>, String) = match &target {
        Some(t) => (Arc::from(open_store(t).expect("open store")), t.clone()),
        None => {
            tempdir = tempfile::tempdir().expect("tempdir");
            (
                Arc::new(LocalFsStore::new(tempdir.path())),
                "local fs temp dir".to_string(),
            )
        }
    };

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    println!("commit latency, {samples} sequential {RECORD_BYTES} B commits per tier, {backend}");

    type MakeTarget = fn(Arc<dyn CasStore>) -> Arc<dyn WalTarget>;
    let tiers: [(&str, MakeTarget); 3] = [
        ("pure_s3", |s| Arc::new(PureS3Target::new(s))),
        ("express", |s| Arc::new(ExpressTarget::new(s))),
        ("buffered", |s| Arc::new(BufferedTarget::new(s))),
    ];
    for (name, make) in tiers {
        let tenant = format!("bench-{nonce}-{name}");
        bench(name, Arc::clone(&store), &tenant, warmup, samples, make);
    }
}
