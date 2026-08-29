//! `zou stats <counter-file> [--since <earlier-copy>] [--brief]
//! [--commits <n>]`: dump the store op counters one run accumulated, as
//! json on stdout.
//!
//! The counter file is what `ZOU_STORE_STATS` pointed at, `zou dev`
//! keeps one at `<runtime>/store-stats` and logs the path on boot. The
//! dump is a cold read of the file, so it is safe to run while the
//! store is live and the harness scrapes it after every benchmark run.
//!
//! `--since` names a copy of the same file taken earlier in the run and
//! dumps the difference. A benchmark with phases wants that: a load
//! phase and a read only phase buy different things, and counters that
//! carry both of them rolled together can only say what the whole run
//! cost, which is the number nobody was asking about.
//!
//! `--brief` prints the same numbers as three lines rather than as
//! json, the ops with their bytes, the same ops by what they were
//! carrying, and the read tiers with their p50. That is what a benchmark
//! line wants beside its tps: a tps on its own says which bargain suited
//! the scenario, not what the bargain was. The class line is there
//! because the ops alone do not say it either, a put of a page and a put
//! of a wal chunk are the same op and the whole difference between the
//! two read paths is which of them a phase does. A fourth line appears
//! when the phase had trouble, retries and CAS conflicts and outright
//! failures, because a put that was slow because the bucket asked for
//! less traffic and a put that was slow because the object was large
//! have the same latency histogram and this is the difference.
//!
//! `--commits` names how many transactions committed over the same
//! window, which the harness takes out of `pg_stat_database` at the same
//! phase boundaries, and turns into commits per PUT: the store bill
//! divided by the work done, which is the number group commit exists to
//! move.
//!
//! A last line says what compression bought, per compressor, raw bytes
//! in and stored bytes out. It is the one term of space amplification
//! nobody outside the store can measure, because the objects on the
//! bucket are the compressed ones and a block that did not compress
//! went out raw with nothing to mark it.

use std::path::Path;

use zou_store::stats::{CLASS_NAMES, Snapshot};

pub const USAGE: &str =
    "usage: zou stats <counter-file> [--since <earlier-copy>] [--brief] [--commits <n>]";

pub fn run(argv: &[String]) -> Result<(), String> {
    let mut brief = false;
    let mut commits = 0u64;
    let mut rest: Vec<&String> = Vec::new();
    let mut argv = argv.iter();
    while let Some(arg) = argv.next() {
        if arg == "--brief" {
            brief = true;
        } else if arg == "--commits" {
            let n = argv.next().ok_or_else(|| USAGE.to_string())?;
            commits = n.parse().map_err(|_| format!("{n} is not a number"))?;
        } else {
            rest.push(arg);
        }
    }
    let snapshot = match rest.as_slice() {
        [path] => Snapshot::read(Path::new(path))?,
        [path, flag, earlier] if *flag == "--since" => {
            Snapshot::read_since(Path::new(path), Path::new(earlier))?
        }
        _ => return Err(USAGE.into()),
    };
    if brief {
        say!("{}", brief_lines(&snapshot, commits));
    } else {
        say!("{}", snapshot.to_json());
    }
    Ok(())
}

/// The lines `--brief` prints. Ops that never ran, classes nothing
/// touched and tiers that never answered are left out rather than
/// printed as zeroes, so a leg that paid nothing says so by being
/// short. The trouble line is left out entirely on a run that had none,
/// for the same reason: a clean run should not carry a row of zeroes
/// saying so.
fn brief_lines(snapshot: &Snapshot, commits: u64) -> String {
    let ops: Vec<String> = snapshot
        .ops
        .iter()
        .filter(|o| o.count > 0)
        .map(|o| format!("{} {} {}", o.count, o.op, bytes(o.bytes)))
        .collect();
    let classes: Vec<String> = CLASS_NAMES
        .iter()
        .copied()
        .filter_map(|name| {
            let (mut count, mut size) = (0u64, 0u64);
            for class in snapshot.ops.iter().flat_map(|o| o.by_class.iter()) {
                if class.class == name {
                    count += class.count;
                    size += class.bytes;
                }
            }
            (count > 0).then(|| format!("{count} {name} {}", bytes(size)))
        })
        .collect();
    let reads: Vec<String> = snapshot
        .reads
        .iter()
        .filter(|t| t.calls > 0)
        .map(|t| format!("{} {} p50 {} us", t.pages, t.tier, t.p50_us))
        .collect();
    let line = |label: &str, parts: Vec<String>| {
        if parts.is_empty() {
            format!("{label}: none")
        } else {
            format!("{label}: {}", parts.join(", "))
        }
    };
    let mut out = format!(
        "{}\n{}\n{}",
        line("store", ops),
        line("carried", classes),
        line("reads", reads)
    );
    // What the store made us do twice, and what it refused. A phase
    // whose puts were slow because the bucket asked for less traffic
    // and a phase whose puts were slow because the objects were large
    // have the same latency histogram, and this is the difference.
    let mut trouble: Vec<String> = snapshot
        .retries
        .iter()
        .filter(|r| r.count > 0)
        .map(|r| format!("{} {}", r.count, r.kind))
        .collect();
    if snapshot.conflicts > 0 {
        trouble.push(format!("{} cas conflicts", snapshot.conflicts));
    }
    for op in snapshot.ops.iter().filter(|o| o.errors > 0) {
        trouble.push(format!("{} {} errors", op.errors, op.op));
    }
    if !trouble.is_empty() {
        out.push('\n');
        out.push_str(&line("trouble", trouble));
    }
    // How many commits rode one PUT, which is the S3 bill divided by
    // the work done. Group commit is what moves it, and a leg that
    // improved its tps by batching harder should have to say so here.
    // Only printed when the phase actually put something: a read only
    // phase has transactions and no puts, and the division would be a
    // statement about nothing.
    let puts: u64 = snapshot
        .ops
        .iter()
        .filter(|o| o.op == "put" || o.op == "put_if_match")
        .map(|o| o.count)
        .sum();
    if commits > 0 && puts > 0 {
        out.push('\n');
        out.push_str(&format!(
            "commits: {commits} over {puts} puts, {:.2} per put",
            commits as f64 / puts as f64
        ));
    }
    // What compression bought, which is the one term of space
    // amplification that cannot be measured from outside the store: the
    // objects on the bucket are the compressed ones and nothing out
    // there knows what they would have been. Left out when nothing was
    // compressed, like every other line here.
    let packed: Vec<String> = snapshot
        .packed
        .iter()
        .filter(|p| p.raw > 0)
        .map(|p| {
            format!(
                "{} {} into {}, {:.2}x",
                p.kind,
                bytes(p.raw),
                bytes(p.stored),
                p.raw as f64 / p.stored.max(1) as f64
            )
        })
        .collect();
    if !packed.is_empty() {
        out.push('\n');
        out.push_str(&line("packed", packed));
    }
    out
}

/// Bytes at one decimal place, binary units, because the thing being
/// counted is pages and a page is 8 KiB rather than 8000 bytes.
fn bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = n as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit + 1 < UNITS.len() {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{size:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zou_store::stats::{
        ClassSnapshot, GapSnapshot, OpSnapshot, PackSnapshot, RetrySnapshot, Snapshot, TierSnapshot,
    };

    fn snapshot(ops: Vec<OpSnapshot>, reads: Vec<TierSnapshot>) -> Snapshot {
        Snapshot {
            conflicts: 0,
            ops,
            reads,
            pagesvc: Vec::new(),
            commit: Vec::new(),
            park_cause: Vec::new(),
            park_gap: GapSnapshot {
                samples: 0,
                p50_bytes: 0,
                p95_bytes: 0,
                p99_bytes: 0,
                max_bytes: 0,
            },
            retries: Vec::new(),
            packed: Vec::new(),
        }
    }

    fn retry(kind: &'static str, count: u64) -> RetrySnapshot {
        RetrySnapshot { kind, count }
    }

    fn packed(kind: &'static str, raw: u64, stored: u64) -> PackSnapshot {
        PackSnapshot { kind, raw, stored }
    }

    fn op(name: &'static str, by_class: &[(&'static str, u64, u64)]) -> OpSnapshot {
        OpSnapshot {
            op: name,
            count: by_class.iter().map(|c| c.1).sum(),
            bytes: by_class.iter().map(|c| c.2).sum(),
            errors: 0,
            p50_us: 0,
            p95_us: 0,
            p99_us: 0,
            max_us: 0,
            by_class: by_class
                .iter()
                .map(|&(class, count, bytes)| ClassSnapshot {
                    class,
                    count,
                    bytes,
                })
                .collect(),
            buckets: Vec::new(),
        }
    }

    fn tier(name: &'static str, calls: u64, pages: u64, p50_us: u64) -> TierSnapshot {
        TierSnapshot {
            tier: name,
            calls,
            pages,
            p50_us,
            p95_us: 0,
            p99_us: 0,
            max_us: 0,
        }
    }

    /// The lines are what a benchmark phase prints under its tps, so
    /// the ops that never ran have to stay off them, and the class line
    /// has to separate a put of a page from a put of wal because that
    /// is the whole difference between the two read paths.
    #[test]
    fn the_brief_lines_carry_what_was_paid_and_skip_what_was_not() {
        let snap = snapshot(
            vec![
                op("get", &[("page", 3412685, 27_950_000_000)]),
                op(
                    "put",
                    &[("page", 17191, 140_500_000), ("wal", 2787, 22_540_000)],
                ),
                op("delete", &[]),
            ],
            vec![tier("cache", 12, 12, 2), tier("store", 400, 3412685, 16)],
        );
        assert_eq!(
            brief_lines(&snap, 0),
            "store: 3412685 get 26.0 GiB, 19978 put 155.5 MiB\n\
             carried: 2787 wal 21.5 MiB, 3429876 page 26.2 GiB\n\
             reads: 12 cache p50 2 us, 3412685 store p50 16 us"
        );
    }

    /// A leg that touched the store not at all is the interesting half
    /// of the comparison these lines exist for, so it says so.
    #[test]
    fn a_leg_that_paid_nothing_says_none() {
        assert_eq!(
            brief_lines(&snapshot(Vec::new(), Vec::new()), 0),
            "store: none\ncarried: none\nreads: none"
        );
    }

    /// A run that was throttled and a run that was not have to be told
    /// apart at a glance, and a clean run should not carry a line of
    /// zeroes saying it was clean.
    #[test]
    fn trouble_only_appears_when_there_was_some() {
        let mut snap = snapshot(vec![op("put", &[("wal", 40, 320_000)])], Vec::new());
        assert!(!brief_lines(&snap, 0).contains("trouble"));
        snap.retries = vec![
            retry("throttle", 12),
            retry("server", 0),
            retry("exhausted", 1),
        ];
        snap.conflicts = 3;
        assert_eq!(
            brief_lines(&snap, 0)
                .lines()
                .last()
                .expect("a trouble line"),
            "trouble: 12 throttle, 1 exhausted, 3 cas conflicts"
        );
    }

    /// Commits per put is the S3 bill divided by the work done, and a
    /// read only phase has transactions and no puts, where the division
    /// would be a statement about nothing.
    #[test]
    fn commits_per_put_needs_puts() {
        let snap = snapshot(vec![op("put", &[("wal", 40, 320_000)])], Vec::new());
        assert_eq!(
            brief_lines(&snap, 430).lines().last().expect("a line"),
            "commits: 430 over 40 puts, 10.75 per put"
        );
        let read_only = snapshot(vec![op("get", &[("page", 900, 7_372_800)])], Vec::new());
        assert!(!brief_lines(&read_only, 430).contains("commits:"));
    }

    /// Space amplification divides the store by the logical size, and
    /// the store is the compressed bytes, so what compression bought
    /// has to be readable next to it. A kind that compressed nothing
    /// stays off the line rather than reporting 1.00x of zero.
    #[test]
    fn packed_says_what_compression_bought() {
        let mut snap = snapshot(vec![op("put", &[("page", 4, 32_768)])], Vec::new());
        assert!(!brief_lines(&snap, 0).contains("packed"));
        snap.packed = vec![
            packed("layer", 134_217_728, 43_200_000),
            packed("wal", 0, 0),
            packed("file", 1024, 1024),
        ];
        assert_eq!(
            brief_lines(&snap, 0).lines().last().expect("a packed line"),
            "packed: layer 128.0 MiB into 41.2 MiB, 3.11x, file 1.0 KiB into 1.0 KiB, 1.00x"
        );
    }

    #[test]
    fn bytes_read_as_sizes_rather_than_as_digits() {
        assert_eq!(bytes(0), "0 B");
        assert_eq!(bytes(999), "999 B");
        assert_eq!(bytes(8192), "8.0 KiB");
        assert_eq!(bytes(67700 * 8192), "528.9 MiB");
        assert_eq!(bytes(30_700_000_000), "28.6 GiB");
    }
}
