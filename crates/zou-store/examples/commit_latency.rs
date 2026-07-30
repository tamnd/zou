//! Commit latency harness: measures append-to-ack latency per tier.
//!
//! Run: cargo run --release --example commit_latency [runs-dir]
//!
//! Every tier currently runs against LocalFsStore, so the numbers measure
//! the pipeline itself (batching, framing, fsync, rename) rather than any
//! network. S3, Express, and MinIO baselines get recorded when those
//! backends land. Results go to stdout, recorded baselines live in
//! docs/benchmarks.md.

use std::sync::Arc;
use std::time::Instant;

use zou_store::layout::TenantLayout;
use zou_store::tier::{BufferedTarget, ExpressTarget, PureS3Target, WalTarget};
use zou_store::{CasStore, GroupCommit, GroupCommitConfig, LocalFsStore, Lsn};

const WARMUP: usize = 200;
const SAMPLES: usize = 2000;
const RECORD_BYTES: usize = 128;

fn percentile(sorted: &[u128], p: f64) -> u128 {
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

fn bench(name: &str, make_target: impl Fn(Arc<dyn CasStore>) -> Arc<dyn WalTarget>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dir.path()));
    let target = make_target(Arc::clone(&store));
    let gc = GroupCommit::builder(store, TenantLayout::new("bench"))
        .session(1, 1)
        .start_lsn(Lsn(0))
        .config(GroupCommitConfig::default())
        .target(target)
        .build();

    let record = vec![7u8; RECORD_BYTES];
    for _ in 0..WARMUP {
        gc.append(&record).unwrap().wait().unwrap();
    }

    let mut micros: Vec<u128> = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let start = Instant::now();
        gc.append(&record).unwrap().wait().unwrap();
        micros.push(start.elapsed().as_micros());
    }
    gc.close().unwrap();

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
    println!(
        "commit latency, {SAMPLES} sequential {RECORD_BYTES} B commits per tier, local fs backend"
    );
    bench("pure_s3", |s| Arc::new(PureS3Target::new(s)));
    bench("express", |s| Arc::new(ExpressTarget::new(s)));
    bench("buffered", |s| Arc::new(BufferedTarget::new(s)));
}
