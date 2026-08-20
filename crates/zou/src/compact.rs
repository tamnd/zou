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
//!
//! `--horizon` is the other command, the merge fold. The sweep above
//! never drops history: an old image is somebody's base and every
//! record the tenant wrote is in some delta, so the layers grow with
//! the write volume and never shrink. The fold buys the right to drop
//! them by paying for it once, cutting one image that holds every key
//! the layers below it held, and it needs a redo pool to build those
//! pages. It is scheduled in retention windows, not minutes, which is
//! why it is a separate run and not part of the sweep.

use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::time::{SystemTime, UNIX_EPOCH};

use zou_pg::compact::{
    MergeOutcome, READ_AMP_BOUND, debts, horizon_for, merge_to_horizon, run_queue,
};
use zou_pg::gc::DEFAULT_RETENTION_SECS;
use zou_pg::install;
use zou_pg::redo::{RedoPool, RedoPoolConfig};
use zou_pg::restore::store_data_checksums;
use zou_store::lsn::Lsn;
use zou_store::open_store;
use zou_store::shards::prune_lineage;

pub const USAGE: &str = "usage: zou compact <target> <ref> [--workers <n> | --status | --horizon [<lsn>] [--retention <duration>] [--pg-bin <path>] [--data-checksums | --no-data-checksums] [--json]]";

/// A merge fold worker holds a whole image in memory before it cuts it,
/// so these do not stack the way sweep workers do. One tenant at a time
/// is the shape a scheduled fold wants anyway.
const MERGE_REDO_WORKERS: usize = 4;

/// The same cap the page service gives a redo batch. A merge asks for
/// the same work in the same batches, so a batch taking longer than
/// this means the same thing here as it does there.
const MERGE_BATCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Batches a redo worker serves before the pool replaces it, which
/// bounds postgres's invalid page tracking over a fold that walks a
/// tenant's whole history.
const MERGE_BATCHES_PER_WORKER: u64 = 256;

pub fn run(argv: &[String]) -> Result<(), String> {
    let (target, tenant_ref, rest) = match argv {
        [target, tenant_ref, rest @ ..] => (target, tenant_ref, rest),
        _ => return Err(USAGE.into()),
    };
    if rest.first().is_some_and(|f| f == "--horizon") {
        return fold(target, tenant_ref, &rest[1..]);
    }
    let workers = match rest {
        [] => 4,
        [flag] if flag == "--status" => {
            // Read only: the debt table as JSON, one row per shard, so a
            // harness can watch the read amp bound during a run without
            // triggering work. The queue order is the scheduler's.
            let store = open_store(target)?;
            let jobs = debts(&*store, tenant_ref).map_err(|e| e.to_string())?;
            let rows: Vec<String> = jobs
                .iter()
                .map(|job| {
                    format!(
                        "{{\"shard\":{},\"debt\":{},\"amp\":{},\"bound\":{}}}",
                        job.shard, job.debt, job.amp, READ_AMP_BOUND
                    )
                })
                .collect();
            println!("[{}]", rows.join(","));
            return Ok(());
        }
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
                "shard {}: {} layers into {}, debt {} to {}, imaged {} pages of which {} off the frozen objects",
                job.shard,
                out.retired,
                out.outputs,
                out.debt_before,
                out.debt_after,
                out.imaged,
                out.frozen
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

/// What `--horizon` was told, before the store gets a look at it.
#[derive(Debug, PartialEq, Eq)]
struct Fold {
    /// An lsn named on the command line, which overrides the policy.
    /// Nothing checks it against the checkpoints, so this is the flag
    /// that lets an operator fold above a restore point on purpose.
    at: Option<Lsn>,
    retention_secs: u64,
    pg_bin: Option<PathBuf>,
    /// An override for what the store says about data checksums.
    /// Nothing should need this: the setting is fixed at initdb and
    /// the store carries the answer. It exists for a store whose
    /// captures have been collected out from under it, where the fold
    /// would otherwise refuse to run at all.
    data_checksums: Option<bool>,
    /// Report the fold as one JSON array instead of prose, the same
    /// shape `--status` reports the debt table in. A fold on a cadence
    /// is the only thing that ever shrinks a tenant's page layers, so
    /// a soak that runs one wants the numbers in its result file, not
    /// in a sentence somebody has to read.
    json: bool,
}

fn parse_fold(argv: &[String]) -> Result<Fold, String> {
    let mut fold = Fold {
        at: None,
        retention_secs: DEFAULT_RETENTION_SECS,
        pg_bin: None,
        data_checksums: None,
        json: false,
    };
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--retention" => {
                fold.retention_secs = crate::gc::secs(
                    it.next()
                        .ok_or_else(|| "--retention needs a value".to_string())?,
                )?
            }
            "--pg-bin" => {
                fold.pg_bin = Some(PathBuf::from(
                    it.next()
                        .ok_or_else(|| "--pg-bin needs a value".to_string())?,
                ))
            }
            "--data-checksums" => fold.data_checksums = Some(true),
            "--no-data-checksums" => fold.data_checksums = Some(false),
            "--json" => fold.json = true,
            other if fold.at.is_none() && !other.starts_with('-') => {
                fold.at = Some(
                    other
                        .parse()
                        // The lsn parse names the value and the form it
                        // wanted, so repeating the value here would say
                        // it twice in one line.
                        .map_err(|e| format!("bad horizon: {e}"))?,
                )
            }
            other => return Err(format!("unexpected argument {other:?}\n{USAGE}")),
        }
    }
    Ok(fold)
}

fn fold(target: &str, tenant_ref: &str, argv: &[String]) -> Result<(), String> {
    let args = parse_fold(argv)?;
    let pg_bin = install::pg_bin(args.pg_bin);
    let postgres = pg_bin.join("postgres");
    if !postgres.is_file() {
        return Err(format!(
            "{} not found, point --pg-bin or ZOU_PG_BIN at a patched install",
            postgres.display()
        ));
    }
    let store = open_store(target)?;
    // A page built without the checksum a checksummed cluster expects
    // reads back as a verification failure, and the read that finds it
    // is usually recovery, so the fold asks the store instead of
    // defaulting. Guessing wrong here is a server that will not start,
    // which is exactly how it was found.
    let data_checksums = match args.data_checksums {
        Some(on) => on,
        None => store_data_checksums(&*store, tenant_ref)?,
    };
    let at = match args.at {
        Some(at) => at,
        None => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| "the clock is before 1970".to_string())?
                .as_secs();
            horizon_for(&*store, tenant_ref, now, args.retention_secs).map_err(|e| e.to_string())?
        }
    };
    if !args.json {
        println!(
            "folding {tenant_ref} to {at}, {}, data checksums {}",
            match args.at {
                Some(_) => "named on the command line".to_string(),
                None => format!(
                    "the oldest lsn a checkpoint still names inside {}",
                    crate::gc::span(args.retention_secs)
                ),
            },
            if data_checksums { "on" } else { "off" }
        );
    }
    let jobs = debts(&*store, tenant_ref).map_err(|e| e.to_string())?;
    let pool = RedoPool::new(RedoPoolConfig {
        postgres,
        scratch_root: std::env::temp_dir(),
        workers: MERGE_REDO_WORKERS,
        batch_timeout: MERGE_BATCH_TIMEOUT,
        batches_per_worker: MERGE_BATCHES_PER_WORKER,
        data_checksums,
    });
    // One shard at a time. Each fold holds an image in memory while it
    // fills, and the point of the whole exercise is a store bigger than
    // the box it runs on.
    let mut failed = 0;
    let mut rows: Vec<String> = Vec::new();
    for job in &jobs {
        match merge_to_horizon(&*store, tenant_ref, job.shard, at, &pool, data_checksums) {
            Ok(Some(out)) if args.json => rows.push(fold_row(job.shard, &out)),
            Ok(Some(out)) => println!(
                "shard {}: {} layers retired into {} at {}, {} pages imaged, {} keys with no base kept {} layers alive, {} to {} bytes",
                job.shard,
                out.retired,
                out.outputs,
                out.horizon,
                out.imaged,
                out.unbased,
                out.pinned,
                out.bytes_before,
                out.bytes_after
            ),
            Ok(None) if args.json => {}
            Ok(None) => println!("shard {}: nothing below the horizon to fold", job.shard),
            Err(e) => {
                failed += 1;
                eprintln!("shard {}: {e}", job.shard);
            }
        }
    }
    // The horizon goes out even when no shard moved, because where a
    // fold decided it was allowed to cut is the answer to why it did
    // nothing. A shard that had nothing below the horizon is simply
    // absent from the array; the exit status is what says the fold ran
    // at all. The checksum setting goes out with it because a fold that
    // read it wrong writes pages nothing can read back, and a soak that
    // recorded what the fold believed can say so afterwards.
    if args.json {
        println!(
            "{{\"horizon\":\"{at}\",\"data_checksums\":{data_checksums},\"shards\":[{}]}}",
            rows.join(",")
        );
    }
    if failed > 0 {
        return Err(format!("{failed} of {} shards failed", jobs.len()));
    }
    Ok(())
}

/// One shard's fold as a JSON object. `bytes_before` and `bytes_after`
/// are the shard's whole layer footprint either side of the fold, not
/// the bytes this pass touched, so a soak can plot the store size the
/// fold is there to hold down.
fn fold_row(shard: u16, out: &MergeOutcome) -> String {
    format!(
        "{{\"shard\":{},\"horizon\":\"{}\",\"retired\":{},\"outputs\":{},\"imaged\":{},\"unbased\":{},\"pinned\":{},\"bytes_before\":{},\"bytes_after\":{}}}",
        shard,
        out.horizon,
        out.retired,
        out.outputs,
        out.imaged,
        out.unbased,
        out.pinned,
        out.bytes_before,
        out.bytes_after
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_fold_defaults_to_the_gc_retention_and_no_named_lsn() {
        let got = parse_fold(&argv(&[])).unwrap();
        assert_eq!(got.at, None);
        assert_eq!(got.retention_secs, DEFAULT_RETENTION_SECS);
        assert_eq!(got.data_checksums, None);
    }

    #[test]
    fn a_named_lsn_reads_the_way_postgres_writes_one() {
        let got = parse_fold(&argv(&["0/8B000000", "--data-checksums"])).unwrap();
        assert_eq!(got.at, Some("0/8B000000".parse().unwrap()));
        assert_eq!(got.data_checksums, Some(true));
    }

    #[test]
    fn the_retention_and_the_binary_come_off_the_flags() {
        let got = parse_fold(&argv(&["--retention", "10m", "--pg-bin", "/opt/pg/bin"])).unwrap();
        assert_eq!(got.retention_secs, 600);
        assert_eq!(got.pg_bin, Some(PathBuf::from("/opt/pg/bin")));
    }

    /// Neither flag means the store gets asked, which is the whole
    /// point: the setting is fixed at initdb and a fold that guessed
    /// it wrong wrote pages a checksummed cluster refused to start on.
    /// The flags are there for a store whose captures are gone.
    #[test]
    fn the_checksum_setting_comes_off_the_store_unless_a_flag_overrules_it() {
        assert_eq!(parse_fold(&argv(&[])).unwrap().data_checksums, None);
        assert_eq!(
            parse_fold(&argv(&["--no-data-checksums"]))
                .unwrap()
                .data_checksums,
            Some(false)
        );
    }

    #[test]
    fn the_json_flag_is_off_until_it_is_asked_for() {
        assert!(!parse_fold(&argv(&[])).unwrap().json);
        let got = parse_fold(&argv(&["--json", "--retention", "10m"])).unwrap();
        assert!(got.json);
        assert_eq!(got.retention_secs, 600);
    }

    #[test]
    fn a_fold_row_carries_the_footprint_either_side_of_the_cut() {
        let out = MergeOutcome {
            horizon: Lsn(0x8B00_0000),
            retired: 12,
            outputs: 2,
            imaged: 3400,
            unbased: 1,
            pinned: 1,
            bytes_before: 136_770_905,
            bytes_after: 8_715_593,
        };
        let row = fold_row(3, &out);
        assert!(row.contains("\"shard\":3"), "{row}");
        assert!(row.contains("\"horizon\":\"0/8B000000\""), "{row}");
        assert!(row.contains("\"bytes_before\":136770905"), "{row}");
        assert!(row.contains("\"bytes_after\":8715593"), "{row}");
        // The shape has to survive a round trip, because the whole
        // point of the flag is that something else reads it.
        assert_eq!(row.matches('{').count(), row.matches('}').count());
    }

    #[test]
    fn a_word_that_is_not_an_lsn_is_a_refusal_not_a_guess() {
        assert!(parse_fold(&argv(&["soon"])).is_err());
        assert!(parse_fold(&argv(&["--retention"])).is_err());
        assert!(parse_fold(&argv(&["--workers", "4"])).is_err());
    }
}
