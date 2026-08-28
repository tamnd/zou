//! `zou probe <target>`: what this machine's distance to that store is.
//!
//! Every number in the result book is a number about a pair, a box and
//! a store, and the pair is usually left out. A tps measured against a
//! MinIO on the same host and a tps measured against a bucket an ocean
//! away are the same column in the table and not the same measurement,
//! and the only honest way to tell them apart later is to have written
//! down what the distance was on the day.
//!
//! So this is the row a result gets stamped with. A small object round
//! tripped many times is the latency, since a page read and a manifest
//! swap are both one small object and the store's service time is what
//! they wait on. A large object moved a few times is the bandwidth,
//! since a layer fetch and a checkpoint upload are bytes and the wire
//! is what they wait on. Both go through the same store client the
//! engine uses, so what comes out includes the signing, the http
//! client and the retries: the cost as paid rather than as advertised.
//!
//! It writes. A handful of objects go under `probe/` and are deleted
//! on the way out, which is the only way to measure a put at all. A
//! store that must not be written to is a store to run this against a
//! copy of.

use std::time::{Duration, Instant};

use zou_store::cas::CasStore;
use zou_store::open_store;

pub const USAGE: &str =
    "usage: zou probe <target> [--rounds <n>] [--size <bytes>] [--large <bytes>] [--json]";

/// How many small round trips make the latency figure. Enough that a
/// p95 means something and few enough that probing a slow store is
/// still a thing done while waiting.
const ROUNDS: usize = 30;

/// The small object, which is a page. What a page read moves is what
/// the latency number should be measured with.
const SMALL: usize = 8192;

/// The large object, sized like a layer rather than like a page.
const LARGE: usize = 8 * 1024 * 1024;

/// How many times the large object is moved. Three, because the first
/// one is a connection being opened and the median of three says so
/// without a warmup round nobody would believe.
const LARGE_ROUNDS: usize = 3;

#[derive(Debug)]
struct Args {
    target: String,
    rounds: usize,
    small: usize,
    large: usize,
    json: bool,
}

pub fn run(argv: &[String]) -> Result<(), String> {
    let args = parse(argv)?;
    let store = open_store(&args.target)?;
    let probe = measure(&*store, &args)?;
    if args.json {
        say!("{}", probe.to_json(&args.target));
    } else {
        say!("{}", probe.lines(&args.target));
    }
    Ok(())
}

fn parse(argv: &[String]) -> Result<Args, String> {
    let mut args = Args {
        target: String::new(),
        rounds: ROUNDS,
        small: SMALL,
        large: LARGE,
        json: false,
    };
    let mut rest = argv.iter();
    while let Some(arg) = rest.next() {
        let mut number = |what: &str| -> Result<usize, String> {
            rest.next()
                .ok_or_else(|| format!("{what} wants a number"))?
                .parse::<usize>()
                .map_err(|_| format!("{what} wants a number"))
        };
        match arg.as_str() {
            "--rounds" => args.rounds = number("--rounds")?,
            "--size" => args.small = number("--size")?,
            "--large" => args.large = number("--large")?,
            "--json" => args.json = true,
            _ if arg.starts_with('-') => return Err(USAGE.into()),
            _ if args.target.is_empty() => args.target = arg.clone(),
            _ => return Err(USAGE.into()),
        }
    }
    if args.target.is_empty() {
        return Err(USAGE.into());
    }
    if args.rounds == 0 || args.small == 0 || args.large == 0 {
        return Err("a probe of nothing measures nothing".into());
    }
    Ok(args)
}

/// One op's samples, kept rather than folded, because a p95 of thirty
/// round trips is a sort and the sort is cheaper than the argument
/// about which streaming estimator was right.
struct Op {
    name: &'static str,
    took: Vec<Duration>,
}

impl Op {
    fn new(name: &'static str) -> Op {
        Op {
            name,
            took: Vec::new(),
        }
    }

    fn at(&self, quantile: f64) -> Duration {
        if self.took.is_empty() {
            return Duration::ZERO;
        }
        let mut sorted = self.took.clone();
        sorted.sort_unstable();
        let last = sorted.len() - 1;
        sorted[((sorted.len() as f64 * quantile) as usize).min(last)]
    }

    fn total(&self) -> Duration {
        self.took.iter().sum()
    }
}

struct Probe {
    small: Vec<Op>,
    large: Vec<Op>,
    small_bytes: usize,
    large_bytes: usize,
    rounds: usize,
}

/// The four calls the engine makes, timed one at a time.
///
/// One at a time and in this order on purpose. A get straight after
/// the put that wrote it is the read a page service does, and a store
/// that answers that one out of a cache in front of it is a store
/// whose number should say so rather than be hidden by a shuffle.
fn measure(store: &dyn CasStore, args: &Args) -> Result<Probe, String> {
    let prefix = format!("probe/{}", std::process::id());
    let mut small = vec![Op::new("put"), Op::new("get"), Op::new("list")];
    let mut large = vec![Op::new("put"), Op::new("get")];
    let payload = vec![0x5au8; args.small];
    let mut wrote = Vec::new();

    let out = (|| -> Result<(), String> {
        for round in 0..args.rounds {
            let key = format!("{prefix}/small-{round}");
            let at = Instant::now();
            store
                .put_if_absent(&key, &payload)
                .map_err(|e| format!("put {key}: {e}"))?;
            small[0].took.push(at.elapsed());
            wrote.push((key.clone(), true));

            let at = Instant::now();
            let got = store.get(&key).map_err(|e| format!("get {key}: {e}"))?;
            small[1].took.push(at.elapsed());
            match got {
                Some((bytes, _)) if bytes.len() == args.small => {}
                Some((bytes, _)) => {
                    return Err(format!(
                        "the store answered {} bytes for an object of {}",
                        bytes.len(),
                        args.small
                    ));
                }
                None => return Err(format!("the store lost {key} between a put and a get")),
            }

            let at = Instant::now();
            store
                .list(&prefix)
                .map_err(|e| format!("list {prefix}: {e}"))?;
            small[2].took.push(at.elapsed());
        }

        let payload = vec![0x5au8; args.large];
        for round in 0..LARGE_ROUNDS {
            let key = format!("{prefix}/large-{round}");
            let at = Instant::now();
            store
                .put_if_absent(&key, &payload)
                .map_err(|e| format!("put {key}: {e}"))?;
            large[0].took.push(at.elapsed());
            wrote.push((key.clone(), false));

            let at = Instant::now();
            store.get(&key).map_err(|e| format!("get {key}: {e}"))?;
            large[1].took.push(at.elapsed());
        }
        Ok(())
    })();

    // Whatever happened, what was written goes. A probe that failed
    // halfway through and left its objects behind would be a probe
    // nobody runs twice against the same store.
    //
    // Only the small ones are timed, so that the delete samples are a
    // round each like the other three. A delete of eight megabytes and
    // a delete of eight kilobytes are the same call on every backend
    // here, but a column mixing them would have to say so.
    let mut deleting = Op::new("delete");
    for (key, timed) in &wrote {
        let at = Instant::now();
        let _ = store.delete(key);
        if *timed {
            deleting.took.push(at.elapsed());
        }
    }
    out?;
    small.push(deleting);

    Ok(Probe {
        small,
        large,
        small_bytes: args.small,
        large_bytes: args.large,
        rounds: args.rounds,
    })
}

impl Probe {
    /// Bytes a second for one of the large ops, out of its own total
    /// rather than out of the run's, so a put rate and a get rate are
    /// two numbers and not one averaged.
    fn rate(&self, op: &Op) -> f64 {
        let seconds = op.total().as_secs_f64();
        if seconds <= 0.0 {
            return 0.0;
        }
        (op.took.len() * self.large_bytes) as f64 / seconds
    }

    fn lines(&self, target: &str) -> String {
        let latency: Vec<String> = self
            .small
            .iter()
            .map(|op| format!("{} p50 {} p95 {}", op.name, ms(op.at(0.5)), ms(op.at(0.95))))
            .collect();
        let bandwidth: Vec<String> = self
            .large
            .iter()
            .map(|op| {
                format!(
                    "{} {}/s p50 {}",
                    op.name,
                    bytes(self.rate(op) as usize),
                    ms(op.at(0.5))
                )
            })
            .collect();
        format!(
            "target: {target}\nlatency, {} x {}: {}\nbandwidth, {} x {}: {}",
            bytes(self.small_bytes),
            self.rounds,
            latency.join(", "),
            bytes(self.large_bytes),
            LARGE_ROUNDS,
            bandwidth.join(", "),
        )
    }

    fn to_json(&self, target: &str) -> String {
        let mut out = format!("{{\"target\":{},\"rounds\":{}", quoted(target), self.rounds);
        for op in &self.small {
            out.push_str(&format!(
                ",\"{}_p50_us\":{},\"{}_p95_us\":{}",
                op.name,
                op.at(0.5).as_micros(),
                op.name,
                op.at(0.95).as_micros()
            ));
        }
        for op in &self.large {
            out.push_str(&format!(
                ",\"large_{}_bytes_per_second\":{:.0}",
                op.name,
                self.rate(op)
            ));
        }
        out.push_str(&format!(
            ",\"small_bytes\":{},\"large_bytes\":{}}}",
            self.small_bytes, self.large_bytes
        ));
        out
    }
}

fn quoted(text: &str) -> String {
    let escaped: String = text
        .chars()
        .flat_map(|c| match c {
            '"' => vec!['\\', '"'],
            '\\' => vec!['\\', '\\'],
            c => vec![c],
        })
        .collect();
    format!("\"{escaped}\"")
}

/// A duration where the interesting digits are, which is not the same
/// place for a local file and for a bucket in another region.
fn ms(took: Duration) -> String {
    let us = took.as_micros();
    if us < 1000 {
        format!("{us} us")
    } else {
        format!("{:.1} ms", took.as_secs_f64() * 1000.0)
    }
}

/// Powers of two with the names for powers of two, which is what
/// `zou stats` prints, so two lines of one run's output are in the
/// same units.
fn bytes(n: usize) -> String {
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

    fn arg(text: &str) -> String {
        text.to_string()
    }

    #[test]
    fn a_probe_wants_a_target_and_takes_its_shape_from_flags() {
        let args = parse(&[arg("/tmp/store")]).expect("a target is enough");
        assert_eq!(args.target, "/tmp/store");
        assert_eq!(args.rounds, ROUNDS);
        assert_eq!(args.small, SMALL);

        let args = parse(&[
            arg("s3://bucket/prefix"),
            arg("--rounds"),
            arg("5"),
            arg("--size"),
            arg("4096"),
            arg("--json"),
        ])
        .expect("flags parse");
        assert_eq!(args.rounds, 5);
        assert_eq!(args.small, 4096);
        assert!(args.json);

        assert_eq!(parse(&[]).unwrap_err(), USAGE);
        assert_eq!(
            parse(&[arg("/tmp"), arg("--rounds")]).unwrap_err(),
            "--rounds wants a number"
        );
        assert!(parse(&[arg("/tmp"), arg("--rounds"), arg("0")]).is_err());
    }

    #[test]
    fn a_probe_of_a_directory_measures_it_and_leaves_nothing_behind() {
        let dir = std::env::temp_dir().join(format!("zou-probe-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch");
        let target = dir.display().to_string();
        let store = open_store(&target).expect("a directory is a store");
        let args = Args {
            target: target.clone(),
            rounds: 3,
            small: 1024,
            large: 64 * 1024,
            json: false,
        };
        let probe = measure(&*store, &args).expect("the probe runs");

        let named: Vec<&str> = probe.small.iter().map(|op| op.name).collect();
        assert_eq!(named, vec!["put", "get", "list", "delete"]);
        assert_eq!(probe.small[0].took.len(), 3, "one sample a round");
        assert!(probe.rate(&probe.large[1]) > 0.0, "a get moved bytes");

        assert!(
            store.list("probe/").expect("list").is_empty(),
            "a probe that leaves its objects behind is a probe run once"
        );
        let lines = probe.lines(&target);
        assert!(lines.contains("latency, 1.0 KiB x 3: put p50 "), "{lines}");
        assert!(lines.contains("bandwidth, 64.0 KiB x 3: put "), "{lines}");
        assert!(probe.to_json(&target).starts_with('{'));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_quantile_of_nothing_is_zero_rather_than_a_panic() {
        let empty = Op::new("put");
        assert_eq!(empty.at(0.5), Duration::ZERO);
        let mut one = Op::new("get");
        one.took.push(Duration::from_millis(7));
        assert_eq!(one.at(0.95), Duration::from_millis(7));
    }

    #[test]
    fn durations_and_sizes_are_written_where_the_digits_are() {
        assert_eq!(ms(Duration::from_micros(120)), "120 us");
        assert_eq!(ms(Duration::from_micros(12500)), "12.5 ms");
        assert_eq!(bytes(512), "512 B");
        assert_eq!(bytes(8192), "8.0 KiB");
        assert_eq!(bytes(8 * 1024 * 1024), "8.0 MiB");
    }
}
