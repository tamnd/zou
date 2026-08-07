//! `zou compact <target> <ref>`: run a compaction sweep over a
//! tenant's shards.
//!
//! Ranks every shard by debt, the delta bytes reads still pay for,
//! and drains the queue worst first on parallel workers. Each shard
//! commits with one CAS, so killing the sweep anywhere only leaves
//! orphan objects for gc and a rerun picks up the rest, which is the
//! whole preemption story. Runs without a redo pool, so it merges and
//! separates but does not materialize fresh images; the pageserver's
//! background loop owns that half.
//!
//! When every shard covers its own keyspace after the sweep, the
//! split lineage is pruned in the same breath.

use std::sync::atomic::AtomicBool;

use zou_pg::compact::{debts, run_queue};
use zou_store::open_store;
use zou_store::shards::prune_lineage;

pub const USAGE: &str = "usage: zou compact <target> <ref> [--workers <n>]";

pub fn run(argv: &[String]) -> Result<(), String> {
    let (target, tenant_ref, rest) = match argv {
        [target, tenant_ref, rest @ ..] => (target, tenant_ref, rest),
        _ => return Err(USAGE.into()),
    };
    let workers = match rest {
        [] => 4,
        [flag, n] if flag == "--workers" => n.parse().map_err(|_| USAGE.to_string())?,
        _ => return Err(USAGE.into()),
    };
    let store = open_store(target)?;
    let jobs = debts(&*store, tenant_ref).map_err(|e| e.to_string())?;
    let stop = AtomicBool::new(false);
    let results = run_queue(&*store, jobs, workers, &stop, None, false);
    let mut failed = 0;
    for (job, result) in &results {
        match result {
            Ok(Some(out)) => println!(
                "shard {}: {} layers into {}, debt {} to {}",
                job.shard, out.retired, out.outputs, out.debt_before, out.debt_after
            ),
            Ok(None) => println!("shard {}: nothing to do", job.shard),
            Err(e) => {
                failed += 1;
                eprintln!("shard {}: {e}", job.shard);
            }
        }
    }
    if failed > 0 {
        return Err(format!("{failed} of {} shards failed", results.len()));
    }
    if prune_lineage(&*store, tenant_ref).map_err(|e| e.to_string())? {
        println!("lineage clear, every shard stands alone");
    } else {
        println!("lineage kept, some shard still leans on its ancestors");
    }
    Ok(())
}
