//! `zou stats <counter-file> [--since <earlier-copy>] [--brief]`: dump
//! the store op counters one run accumulated, as json on stdout.
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
//! two read paths is which of them a phase does.

use std::path::Path;

use zou_store::stats::{CLASS_NAMES, Snapshot};

pub const USAGE: &str = "usage: zou stats <counter-file> [--since <earlier-copy>] [--brief]";

pub fn run(argv: &[String]) -> Result<(), String> {
    let mut brief = false;
    let mut rest: Vec<&String> = Vec::new();
    for arg in argv {
        if arg == "--brief" {
            brief = true;
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
        say!("{}", brief_lines(&snapshot));
    } else {
        say!("{}", snapshot.to_json());
    }
    Ok(())
}

/// The three lines `--brief` prints. Ops that never ran, classes
/// nothing touched and tiers that never answered are left out rather
/// than printed as zeroes, so a leg that paid nothing says so by being
/// short.
fn brief_lines(snapshot: &Snapshot) -> String {
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
    format!(
        "{}\n{}\n{}",
        line("store", ops),
        line("carried", classes),
        line("reads", reads)
    )
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
    use zou_store::stats::{ClassSnapshot, GapSnapshot, OpSnapshot, Snapshot, TierSnapshot};

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
        }
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
            brief_lines(&snap),
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
            brief_lines(&snapshot(Vec::new(), Vec::new())),
            "store: none\ncarried: none\nreads: none"
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
