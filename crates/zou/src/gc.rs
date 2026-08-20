//! `zou gc <target>`: delete what no retained manifest references.
//!
//! The store only grows on its own. A checkpoint fold supersedes the
//! chain under it, a branch that was deleted leaves its captures, and
//! nothing removes either until a sweep walks the store, pins whatever
//! a live manifest or a retained history snapshot references, and
//! collects the rest.
//!
//! Two numbers are the whole policy. The retention window is how far
//! back point in time recovery reaches, so it is a promise to whoever
//! owns the data. The safety window is how long a key that looks like
//! garbage waits before it is deleted, so it is a promise to whoever is
//! mid publish: a run stamps candidates, a later run deletes the ones
//! that were still garbage on its own scan, and a branch published in
//! between takes its objects back off the list. Deleting anything takes
//! two runs by construction, whatever the windows say.
//!
//! The two are not independent. A stamp is only trusted while the
//! history that would contradict it is still retained, so the retention
//! has to be the longer of the two or no stamp ever comes of age and
//! the sweep frees nothing. That is refused here rather than found out
//! from a disk graph.
//!
//! One sweep runs at a time across the whole deployment, which a lock
//! object in the store enforces rather than an operator remembering.
//! `--dry-run` is outside that: it writes nothing, so it answers what
//! would go without waiting for anybody.

use std::time::{SystemTime, UNIX_EPOCH};

use zou_pg::gc::{self, Policy, Sweep};
use zou_store::open_store;

pub const USAGE: &str = "usage: zou gc <target> [--window <duration>] [--retention <duration>] [--lock-ttl <duration>] [--dry-run] [--force]";

pub fn run(argv: &[String]) -> Result<(), String> {
    let (target, policy) = parse(argv)?;
    let store = open_store(&target)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "the clock is before 1970".to_string())?
        .as_secs();
    match gc::sweep(&*store, &holder(), now, policy)? {
        Sweep::Ran(stats) => {
            for key in &stats.doomed {
                println!("would delete {key}");
            }
            let did = if policy.dry_run {
                "would delete"
            } else {
                "deleted"
            };
            println!(
                "{} tenants, {} {} objects, {} waiting out the {} window",
                stats.tenants,
                did,
                stats.deleted,
                stats.candidates,
                span(policy.window_secs),
            );
            Ok(())
        }
        // A refusal rather than a quiet zero, because a cron entry that
        // never runs and never says so is how a store fills up.
        Sweep::Busy { holder, until_unix } => Err(format!(
            "a sweep is already running, held by {holder} until unix {until_unix}, pass --force if that is not true"
        )),
    }
}

fn parse(argv: &[String]) -> Result<(String, Policy), String> {
    let mut target = None;
    let mut policy = Policy::default();
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--window" => policy.window_secs = secs(need(&mut it, "--window")?)?,
            "--retention" => policy.retention_secs = secs(need(&mut it, "--retention")?)?,
            "--lock-ttl" => policy.lock_ttl_secs = secs(need(&mut it, "--lock-ttl")?)?,
            "--dry-run" => policy.dry_run = true,
            "--force" => policy.force = true,
            other if target.is_none() && !other.starts_with('-') => {
                target = Some(other.to_string());
            }
            other => return Err(format!("unexpected argument {other:?}\n{USAGE}")),
        }
    }
    // A candidate stamp is only trusted while the history snapshots
    // that would contradict it are still around, so a retention no
    // longer than the window leaves every stamp restamped forever and
    // the sweep deletes nothing. Saying so here is better than a sweep
    // that runs clean every night and never frees a byte.
    if policy.retention_secs <= policy.window_secs {
        return Err(format!(
            "--retention {} must be longer than --window {}, or nothing is ever collected",
            span(policy.retention_secs),
            span(policy.window_secs),
        ));
    }
    Ok((target.ok_or(USAGE)?, policy))
}

fn need<'a>(it: &mut std::slice::Iter<'a, String>, flag: &str) -> Result<&'a String, String> {
    it.next().ok_or_else(|| format!("{flag} needs a value"))
}

/// A length of time, written the way a person writes one: a number with
/// `s`, `m`, `h` or `d` on it, or a plain number of seconds.
///
/// One unit, not a run of them, because every duration on this command
/// is a policy somebody chose and none of them is 1h30m.
pub fn secs(raw: &str) -> Result<u64, String> {
    let raw = raw.trim();
    let (digits, scale) = match raw.chars().last() {
        Some('s') => (&raw[..raw.len() - 1], 1u64),
        Some('m') => (&raw[..raw.len() - 1], 60),
        Some('h') => (&raw[..raw.len() - 1], 60 * 60),
        Some('d') => (&raw[..raw.len() - 1], 24 * 60 * 60),
        _ => (raw, 1),
    };
    digits
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(scale))
        .ok_or_else(|| format!("bad duration {raw:?}, write seconds or something like 24h"))
}

/// The same, back the way it was written, so the summary says what the
/// flag said rather than a pile of seconds.
pub fn span(secs: u64) -> String {
    for (unit, size) in [("d", 24 * 60 * 60), ("h", 60 * 60), ("m", 60)] {
        if secs >= size && secs.is_multiple_of(size) {
            return format!("{}{unit}", secs / size);
        }
    }
    format!("{secs}s")
}

/// Who to blame for a lock. `ZOU_NODE_ID` is what a deployment sets and
/// what the wal holder already uses, and a pid is enough for one person
/// at a terminal.
pub fn holder() -> String {
    match std::env::var("ZOU_NODE_ID") {
        Ok(id) if !id.is_empty() => format!("gc-{id}"),
        _ => format!("gc-pid-{}", std::process::id()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn durations_are_written_the_way_people_write_them() {
        assert_eq!(secs("900"), Ok(900));
        assert_eq!(secs("900s"), Ok(900));
        assert_eq!(secs("30m"), Ok(1800));
        assert_eq!(secs("24h"), Ok(86_400));
        assert_eq!(secs("7d"), Ok(604_800));
        assert_eq!(secs(" 7d "), Ok(604_800));
        assert_eq!(secs("0"), Ok(0));
        assert!(secs("").is_err());
        assert!(secs("d").is_err());
        assert!(secs("7 days").is_err());
        assert!(secs("-1").is_err());
        assert!(secs("1w").is_err(), "a unit nobody implemented is refused");
        assert!(secs("99999999999999999999d").is_err(), "and no wrapping");
    }

    #[test]
    fn a_span_reads_back_as_the_flag_that_set_it() {
        assert_eq!(span(604_800), "7d");
        assert_eq!(span(86_400), "1d", "the largest unit that divides it");
        assert_eq!(span(3600), "1h");
        assert_eq!(span(90), "90s");
        assert_eq!(span(0), "0s");
    }

    #[test]
    fn the_defaults_are_a_day_of_window_and_a_week_of_retention() {
        let (target, policy) = parse(&argv(["s3://bucket/fleet"].as_ref())).unwrap();
        assert_eq!(target, "s3://bucket/fleet");
        assert_eq!(policy, Policy::default());
        assert_eq!(policy.window_secs, 24 * 60 * 60);
        assert_eq!(policy.retention_secs, 7 * 24 * 60 * 60);
        assert!(!policy.dry_run);
    }

    #[test]
    fn every_number_is_a_flag_with_a_name_on_it() {
        let (_, policy) = parse(&argv(&[
            "/srv/store",
            "--window",
            "6h",
            "--retention",
            "30d",
            "--lock-ttl",
            "10m",
            "--dry-run",
            "--force",
        ]))
        .unwrap();
        assert_eq!(policy.window_secs, 6 * 60 * 60);
        assert_eq!(policy.retention_secs, 30 * 24 * 60 * 60);
        assert_eq!(policy.lock_ttl_secs, 600);
        assert!(policy.dry_run);
        assert!(policy.force);
    }

    /// The sweep can only trust a candidate stamp while the history
    /// that would contradict it is still retained, so these two numbers
    /// are not independent. A policy that puts them the wrong way round
    /// sweeps every night and frees nothing, which is a thing to be
    /// told at the prompt rather than to work out from a disk graph.
    #[test]
    fn a_retention_no_longer_than_the_window_is_refused_with_both_numbers() {
        let err = parse(&argv(&[
            "/srv/store",
            "--window",
            "6h",
            "--retention",
            "1h",
        ]))
        .expect_err("the policy collects nothing");
        assert!(err.contains("1h") && err.contains("6h"), "{err}");

        assert!(
            parse(&argv(&[
                "/srv/store",
                "--window",
                "6h",
                "--retention",
                "6h"
            ]))
            .is_err(),
            "equal is not longer"
        );
        assert!(
            parse(&argv(&["/srv/store", "--retention", "0s"])).is_err(),
            "no retention at all is the worst case of the same thing"
        );
        parse(&argv(&[
            "/srv/store",
            "--window",
            "1h",
            "--retention",
            "2h",
        ]))
        .expect("longer is fine");
    }

    #[test]
    fn a_sweep_needs_something_to_sweep() {
        assert!(parse(&argv(&[])).is_err());
        assert!(parse(&argv(&["--dry-run"])).is_err());
        assert!(parse(&argv(&["/srv/store", "--window"])).is_err());
        assert!(parse(&argv(&["/srv/store", "--retention", "soon"])).is_err());
        assert!(parse(&argv(&["/srv/store", "--wingow", "6h"])).is_err());
    }
}
