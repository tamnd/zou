//! `zou doctor <target>`: ask a store whether it can hold a database,
//! by doing to it the things the engine does.
//!
//! Every backend claims to be S3 compatible and most of them are, right
//! up to the one operation the whole design rests on. A store that
//! cannot refuse a conditional write looks perfect under a smoke test
//! and loses a manifest the first time two nodes race, and a store that
//! answers a range request with the whole object is correct and quietly
//! ruinous. So the checks here are not a feature list read back, they
//! are the operations run for real against a scratch prefix, with the
//! wrong answer as interesting as the right one: the compare and swap
//! check passes when a stale version is refused, and the create check
//! passes when a second create fails.
//!
//! Nothing is written outside `doctor/<random>/` and everything written
//! is deleted, so this is safe against a live store with a database in
//! it. No lease is taken and no tenant prefix is touched.
//!
//! The clock check is the one that can only see half of its question.
//! A manifest carries the second its holder wrote it, so a timestamp
//! from the future means this node's clock is behind the writer's by at
//! least that much and there is no other reading of it. The other
//! direction has no signal at all: a manifest dated an hour ago is
//! either an hour old or a clock an hour fast, and nothing in the store
//! tells them apart. So the check reports a skew it can prove and says
//! nothing when it cannot, which is the only honest thing it can do.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use zou_store::layout::TenantLayout;
use zou_store::{CasError, CasStore, Manifest, open_store};

pub const USAGE: &str =
    "usage: zou doctor <target> [--tenant <ref>] [--samples <n>] [-o pretty|json]";

/// The probe object's size. Big enough that a range read of the middle
/// of it is a different answer from the whole of it, small enough that
/// the latency probe measures the round trip rather than the transfer.
const PROBE_BYTES: usize = 4096;

/// The slice the range check asks for, chosen so both ends are away
/// from the object's edges: a backend that ignores the offset and one
/// that ignores the length both come back wrong.
const RANGE_AT: u64 = 1024;
const RANGE_LEN: u64 = 256;

/// How far into the future a manifest may be dated before it is called
/// skew. One second of it is ordinary rounding between two hosts that
/// both write whole seconds, two is slack, past that a clock is wrong.
const SKEW_TOLERANCE_SECS: u64 = 2;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Output {
    Pretty,
    Json,
}

#[derive(Debug)]
pub struct Args {
    pub target: String,
    pub tenant: String,
    pub samples: usize,
    pub output: Output,
}

pub fn parse(argv: &[String]) -> Result<Args, String> {
    let mut target = None;
    let mut tenant = "local".to_string();
    let mut samples = 10usize;
    let mut output = Output::Pretty;
    let mut rest = argv.iter();
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "--tenant" => {
                tenant = rest.next().ok_or("--tenant needs a ref")?.clone();
            }
            "--samples" => {
                let raw = rest.next().ok_or("--samples needs a number")?;
                samples = raw
                    .parse()
                    .map_err(|_| format!("--samples wants a number, got {raw}"))?;
                if samples == 0 || samples > 1000 {
                    return Err("--samples wants between 1 and 1000".into());
                }
            }
            "-o" | "--output" => {
                output = match rest.next().map(String::as_str) {
                    Some("pretty") => Output::Pretty,
                    Some("json") => Output::Json,
                    Some(other) => return Err(format!("unknown output {other}")),
                    None => return Err("-o needs pretty or json".into()),
                };
            }
            other if other.starts_with('-') => return Err(format!("unknown flag {other}")),
            other if target.is_none() => target = Some(other.to_string()),
            _ => return Err(USAGE.into()),
        }
    }
    Ok(Args {
        target: target.ok_or(USAGE)?,
        tenant,
        samples,
        output,
    })
}

/// What one check concluded. `Skipped` is not a pass: it means the
/// store did not hold what the check needed to look at, which is worth
/// saying rather than hiding behind a green line.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Status {
    Ok,
    Failed,
    Skipped,
}

impl Status {
    fn word(self) -> &'static str {
        match self {
            Status::Ok => "ok",
            Status::Failed => "failed",
            Status::Skipped => "skipped",
        }
    }
}

#[derive(Debug)]
pub struct Check {
    pub name: &'static str,
    pub status: Status,
    /// What the store actually did, in the words of the operation, so a
    /// failure is a report and not a verdict to go and reproduce.
    pub detail: String,
}

impl Check {
    fn ok(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            status: Status::Ok,
            detail: detail.into(),
        }
    }

    fn failed(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            status: Status::Failed,
            detail: detail.into(),
        }
    }

    fn skipped(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            status: Status::Skipped,
            detail: detail.into(),
        }
    }
}

#[derive(Debug)]
pub struct Report {
    pub target: String,
    pub tenant: String,
    pub prefix: String,
    pub checks: Vec<Check>,
    pub put: Option<Latency>,
    pub get: Option<Latency>,
}

/// One operation's round trip times over the probe, as milliseconds.
#[derive(Debug, Clone, Copy)]
pub struct Latency {
    pub samples: usize,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub max_ms: f64,
}

impl Report {
    fn failures(&self) -> usize {
        self.checks
            .iter()
            .filter(|c| c.status == Status::Failed)
            .count()
    }

    fn pretty(&self) {
        println!("store {}", self.target);
        println!("scratch prefix {}", self.prefix);
        let width = self
            .checks
            .iter()
            .map(|c| c.name.len())
            .max()
            .unwrap_or(0)
            .max(4);
        for check in &self.checks {
            println!(
                "  {:width$}  {:7}  {}",
                check.name,
                check.status.word(),
                check.detail
            );
        }
    }

    fn json(&self) {
        let checks: Vec<_> = self
            .checks
            .iter()
            .map(|c| {
                serde_json::json!({
                    "name": c.name,
                    "status": c.status.word(),
                    "detail": c.detail,
                })
            })
            .collect();
        let latency = |l: &Option<Latency>| {
            l.map(|l| {
                serde_json::json!({
                    "samples": l.samples,
                    "p50_ms": l.p50_ms,
                    "p95_ms": l.p95_ms,
                    "max_ms": l.max_ms,
                })
            })
        };
        let out = serde_json::json!({
            "target": self.target,
            "tenant": self.tenant,
            "prefix": self.prefix,
            "checks": checks,
            "put": latency(&self.put),
            "get": latency(&self.get),
            "ok": self.failures() == 0,
        });
        println!("{out}");
    }
}

pub fn run(argv: &[String]) -> Result<(), String> {
    let args = parse(argv)?;
    let store: Arc<dyn CasStore> =
        Arc::from(open_store(&args.target).map_err(|e| format!("store: {e}"))?);
    let report = diagnose(&*store, &args);
    match args.output {
        Output::Pretty => report.pretty(),
        Output::Json => report.json(),
    }
    // Printed first, the way `zou status` does it, so a caller reading
    // stdout has the whole report whatever the exit code says about it.
    match report.failures() {
        0 => Ok(()),
        1 => Err("1 check failed, this store is not safe to put a database on".into()),
        n => Err(format!(
            "{n} checks failed, this store is not safe to put a database on"
        )),
    }
}

/// The probe's bytes: a pattern rather than a fill, so a read that
/// comes back the right length from the wrong offset is still caught.
fn payload(seed: u8) -> Vec<u8> {
    (0..PROBE_BYTES)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A prefix nothing else writes, so two doctors on the same store at
/// the same time do not see each other's objects and neither of them
/// deletes the other's.
fn scratch_prefix() -> String {
    let mut raw = [0u8; 8];
    if getrandom::fill(&mut raw).is_err() {
        // Not fatal: the id only has to be unlikely to collide, and a
        // clock reading is unlikely enough to keep the run going.
        raw[..8].copy_from_slice(&now_unix().to_le_bytes());
    }
    let id: String = raw.iter().map(|b| format!("{b:02x}")).collect();
    format!("doctor/{id}/")
}

pub fn diagnose(store: &dyn CasStore, args: &Args) -> Report {
    let prefix = scratch_prefix();
    let mut checks = Vec::new();
    let key = format!("{prefix}probe");
    let first = payload(0);
    let second = payload(7);

    match store.list(&prefix) {
        Ok(found) if found.is_empty() => checks.push(Check::ok("list", "the prefix reads empty")),
        Ok(found) => checks.push(Check::ok(
            "list",
            format!("the prefix already holds {} keys", found.len()),
        )),
        Err(e) => checks.push(Check::failed("list", format!("{e}"))),
    }

    // Everything after this needs the object to exist, so a write that
    // fails ends the run rather than reporting a cascade of failures
    // that are all the same one.
    let version = match store.put_if_absent(&key, &first) {
        Ok(v) => {
            checks.push(Check::ok(
                "write",
                format!("{PROBE_BYTES} bytes created at {key}"),
            ));
            v
        }
        Err(e) => {
            checks.push(Check::failed("write", format!("{e}")));
            return Report {
                target: args.target.clone(),
                tenant: args.tenant.clone(),
                prefix,
                checks,
                put: None,
                get: None,
            };
        }
    };

    match store.get(&key) {
        Ok(Some((data, _))) if data == first => checks.push(Check::ok(
            "read back",
            format!("{} bytes, identical", data.len()),
        )),
        Ok(Some((data, _))) => checks.push(Check::failed(
            "read back",
            format!(
                "{} bytes came back and they are not what was written",
                data.len()
            ),
        )),
        Ok(None) => checks.push(Check::failed(
            "read back",
            "the object that was just written is not there",
        )),
        Err(e) => checks.push(Check::failed("read back", format!("{e}"))),
    }

    match store.get_range(&key, RANGE_AT, RANGE_LEN) {
        Ok(Some(data)) if data == first[RANGE_AT as usize..(RANGE_AT + RANGE_LEN) as usize] => {
            checks.push(Check::ok(
                "range read",
                format!("{RANGE_LEN} bytes from offset {RANGE_AT}"),
            ));
        }
        Ok(Some(data)) if data.len() == PROBE_BYTES => checks.push(Check::failed(
            "range read",
            "the whole object came back, this backend ignores the range and every page read pays for it",
        )),
        Ok(Some(data)) => checks.push(Check::failed(
            "range read",
            format!("{} bytes came back instead of {RANGE_LEN} from offset {RANGE_AT}", data.len()),
        )),
        Ok(None) => checks.push(Check::failed("range read", "the object is not there")),
        Err(e) => checks.push(Check::failed("range read", format!("{e}"))),
    }

    // The swap has to happen before the stale one, because what makes a
    // version stale is a write landing on top of it.
    match store.put_if_match(&key, &second, Some(&version)) {
        Ok(_) => checks.push(Check::ok(
            "compare and swap",
            "a write against the current version was taken",
        )),
        Err(e) => checks.push(Check::failed("compare and swap", format!("{e}"))),
    }

    match store.put_if_match(&key, &first, Some(&version)) {
        Err(CasError::Conflict { .. }) => checks.push(Check::ok(
            "stale write refused",
            "a write against the old version was refused, which is what a manifest swap rests on",
        )),
        Ok(_) => checks.push(Check::failed(
            "stale write refused",
            "a write against a version two writes old was taken, this backend has no compare and swap and two nodes will lose a manifest",
        )),
        Err(e) => checks.push(Check::failed("stale write refused", format!("{e}"))),
    }

    match store.put_if_absent(&key, &first) {
        Err(CasError::AlreadyExists { .. }) => checks.push(Check::ok(
            "create refused",
            "a second create of the same key was refused, which is what fences a landing segment",
        )),
        Ok(_) => checks.push(Check::failed(
            "create refused",
            "a create of a key that exists was taken, this backend cannot fence a writer",
        )),
        Err(e) => checks.push(Check::failed("create refused", format!("{e}"))),
    }

    match store.list(&prefix) {
        Ok(found) if found.iter().any(|k| k == &key) => {
            checks.push(Check::ok("listing", "the written key is in the listing"))
        }
        Ok(_) => checks.push(Check::failed(
            "listing",
            "the key that was just written is not in the listing of its own prefix",
        )),
        Err(e) => checks.push(Check::failed("listing", format!("{e}"))),
    }

    let (put, get, latency) = probe_latency(store, &prefix, args.samples);
    checks.push(latency);
    checks.push(clock(store, &args.tenant));

    // Last, so a failure here is about deleting and not about anything
    // the earlier checks left in a strange state.
    checks.push(cleanup(store, &prefix));

    Report {
        target: args.target.clone(),
        tenant: args.tenant.clone(),
        prefix,
        checks,
        put,
        get,
    }
}

/// N put and get round trips on their own keys, so no backend answers
/// the second read out of what it cached for the first.
fn probe_latency(
    store: &dyn CasStore,
    prefix: &str,
    samples: usize,
) -> (Option<Latency>, Option<Latency>, Check) {
    let data = payload(3);
    let mut puts = Vec::with_capacity(samples);
    let mut gets = Vec::with_capacity(samples);
    for i in 0..samples {
        let key = format!("{prefix}latency-{i:04}");
        let at = Instant::now();
        if let Err(e) = store.put(&key, &data) {
            return (None, None, Check::failed("latency", format!("{e}")));
        }
        puts.push(at.elapsed());
        let at = Instant::now();
        match store.get(&key) {
            Ok(Some(_)) => gets.push(at.elapsed()),
            Ok(None) => {
                return (
                    None,
                    None,
                    Check::failed(
                        "latency",
                        format!("{key} was written and read back as absent"),
                    ),
                );
            }
            Err(e) => return (None, None, Check::failed("latency", format!("{e}"))),
        }
    }
    let put = summarise(&mut puts);
    let get = summarise(&mut gets);
    let detail = format!(
        "over {samples} objects of {PROBE_BYTES} bytes, put p50 {:.1} ms p95 {:.1} ms max {:.1} ms, get p50 {:.1} ms p95 {:.1} ms max {:.1} ms",
        put.p50_ms, put.p95_ms, put.max_ms, get.p50_ms, get.p95_ms, get.max_ms
    );
    (Some(put), Some(get), Check::ok("latency", detail))
}

fn summarise(times: &mut [Duration]) -> Latency {
    times.sort_unstable();
    let ms = |d: Duration| d.as_secs_f64() * 1000.0;
    let at = |q: f64| {
        let i = ((times.len() as f64 * q).ceil() as usize).saturating_sub(1);
        ms(times[i.min(times.len() - 1)])
    };
    Latency {
        samples: times.len(),
        p50_ms: at(0.50),
        p95_ms: at(0.95),
        max_ms: ms(times[times.len() - 1]),
    }
}

/// Whether this node's clock is behind the one that last wrote the
/// tenant. See the module note on why only that direction is knowable.
fn clock(store: &dyn CasStore, tenant: &str) -> Check {
    let key = TenantLayout::new(tenant).manifest();
    let manifest = match store.get(&key) {
        Ok(Some((data, _))) => match Manifest::from_json(&data) {
            Ok(m) => m,
            Err(e) => return Check::failed("clock skew", format!("{key}: {e}")),
        },
        Ok(None) => {
            return Check::skipped(
                "clock skew",
                format!("no manifest at {key}, nothing here carries another node's clock"),
            );
        }
        Err(e) => return Check::failed("clock skew", format!("{e}")),
    };
    let Some(published) = manifest.published_unix else {
        return Check::skipped(
            "clock skew",
            format!("the manifest for {tenant} was written before manifests were dated"),
        );
    };
    let now = now_unix();
    let ahead = published.saturating_sub(now);
    if ahead > SKEW_TOLERANCE_SECS {
        return Check::failed(
            "clock skew",
            format!(
                "the manifest for {tenant} is dated {ahead} seconds from now, so this node's clock is behind the writer's by at least that, and a lease taken here would be shorter than it reads"
            ),
        );
    }
    if ahead > 0 {
        return Check::ok(
            "clock skew",
            format!(
                "the manifest for {tenant} is dated {ahead} seconds ahead, inside the rounding two hosts writing whole seconds have"
            ),
        );
    }
    Check::ok(
        "clock skew",
        format!(
            "the manifest for {tenant} was written {} seconds ago, which says this clock is not behind the writer's and says nothing about the other way",
            now - published
        ),
    )
}

/// Delete everything this run wrote and check that it is gone. A store
/// that takes a delete and keeps the object is its own kind of broken,
/// and it is the kind that shows up months later as a bill.
fn cleanup(store: &dyn CasStore, prefix: &str) -> Check {
    let keys = match store.list(prefix) {
        Ok(keys) => keys,
        Err(e) => return Check::failed("cleanup", format!("{e}")),
    };
    let wrote = keys.len();
    for key in &keys {
        if let Err(e) = store.delete(key) {
            return Check::failed("cleanup", format!("{key}: {e}"));
        }
    }
    match store.list(prefix) {
        Ok(left) if left.is_empty() => Check::ok(
            "cleanup",
            format!("{wrote} objects deleted, the prefix reads empty"),
        ),
        Ok(left) => Check::failed(
            "cleanup",
            format!(
                "{} of {wrote} objects are still listed after being deleted",
                left.len()
            ),
        ),
        Err(e) => Check::failed("cleanup", format!("{e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zou_store::{LocalFsStore, Version};

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    fn args_for(dir: &std::path::Path) -> Args {
        Args {
            target: dir.display().to_string(),
            tenant: "local".into(),
            samples: 3,
            output: Output::Json,
        }
    }

    fn status_of<'a>(report: &'a Report, name: &str) -> &'a Check {
        report
            .checks
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("no check named {name}"))
    }

    #[test]
    fn the_flags_come_apart() {
        let args = parse(&argv(&["/tmp/store", "--tenant", "acme", "--samples", "4"])).unwrap();
        assert_eq!(args.target, "/tmp/store");
        assert_eq!(args.tenant, "acme");
        assert_eq!(args.samples, 4);
        assert_eq!(args.output, Output::Pretty);
        assert_eq!(
            parse(&argv(&["/tmp/store", "-o", "json"])).unwrap().output,
            Output::Json
        );
        assert!(parse(&argv(&[])).is_err(), "a target is not optional");
        assert!(parse(&argv(&["/tmp/store", "--samples", "0"])).is_err());
        assert!(parse(&argv(&["/tmp/store", "--samples", "9000"])).is_err());
        assert!(parse(&argv(&["/tmp/store", "-o", "yaml"])).is_err());
        assert!(parse(&argv(&["/tmp/store", "--tenant"])).is_err());
        assert!(parse(&argv(&["/tmp/store", "--nope"])).is_err());
    }

    #[test]
    fn a_directory_passes_every_check_and_is_left_as_it_was() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let report = diagnose(&store, &args_for(dir.path()));
        for check in &report.checks {
            assert_ne!(
                check.status,
                Status::Failed,
                "{} failed: {}",
                check.name,
                check.detail
            );
        }
        assert_eq!(report.failures(), 0);
        assert_eq!(
            status_of(&report, "clock skew").status,
            Status::Skipped,
            "an empty store has no tenant to compare clocks with"
        );
        assert_eq!(report.put.unwrap().samples, 3);
        assert_eq!(report.get.unwrap().samples, 3);
        assert!(
            store.list("doctor/").unwrap().is_empty(),
            "the probe left objects behind"
        );
    }

    /// A store that takes any conditional write is the failure mode this
    /// command exists for, and it is invisible until two nodes race, so
    /// the check is tested against a backend that really does it.
    struct NoCas(LocalFsStore);

    impl CasStore for NoCas {
        fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
            self.0.get(key)
        }
        fn put_if_match(
            &self,
            key: &str,
            data: &[u8],
            _expected: Option<&Version>,
        ) -> Result<Version, CasError> {
            self.0.put(key, data)
        }
        fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
            self.0.put(key, data)
        }
        fn delete(&self, key: &str) -> Result<(), CasError> {
            self.0.delete(key)
        }
        fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
            self.0.list(prefix)
        }
    }

    #[test]
    fn a_store_that_takes_every_conditional_write_is_caught() {
        let dir = tempfile::tempdir().unwrap();
        let store = NoCas(LocalFsStore::new(dir.path()));
        let report = diagnose(&store, &args_for(dir.path()));
        assert_eq!(
            status_of(&report, "stale write refused").status,
            Status::Failed
        );
        assert_eq!(status_of(&report, "create refused").status, Status::Failed);
        assert_eq!(
            status_of(&report, "compare and swap").status,
            Status::Ok,
            "the swap itself works, it is the refusal that does not"
        );
        assert_eq!(report.failures(), 2);
    }

    /// The other quiet one: a backend that answers a range request with
    /// the whole object is correct by the letter and turns every page
    /// read into a full object fetch.
    struct NoRange(LocalFsStore);

    impl CasStore for NoRange {
        fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
            self.0.get(key)
        }
        fn get_range(&self, key: &str, _at: u64, _len: u64) -> Result<Option<Vec<u8>>, CasError> {
            Ok(self.0.get(key)?.map(|(data, _)| data))
        }
        fn put_if_match(
            &self,
            key: &str,
            data: &[u8],
            expected: Option<&Version>,
        ) -> Result<Version, CasError> {
            self.0.put_if_match(key, data, expected)
        }
        fn delete(&self, key: &str) -> Result<(), CasError> {
            self.0.delete(key)
        }
        fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
            self.0.list(prefix)
        }
    }

    #[test]
    fn a_store_that_ignores_the_range_is_caught() {
        let dir = tempfile::tempdir().unwrap();
        let store = NoRange(LocalFsStore::new(dir.path()));
        let report = diagnose(&store, &args_for(dir.path()));
        let range = status_of(&report, "range read");
        assert_eq!(range.status, Status::Failed);
        assert!(
            range.detail.contains("the whole object"),
            "the report should say what it got: {}",
            range.detail
        );
        assert_eq!(report.failures(), 1, "nothing else should have moved");
    }

    #[test]
    fn a_manifest_from_the_future_is_read_as_this_clock_being_behind() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let mut manifest = Manifest::new("local", 18);
        manifest.published_unix = Some(now_unix() + 3600);
        store
            .put(&TenantLayout::new("local").manifest(), &manifest.to_json())
            .unwrap();
        let skew = clock(&store, "local");
        assert_eq!(skew.status, Status::Failed);
        // The number the report carries is the gap between two readings
        // of this clock a moment apart, and the second boundary can
        // fall between them and take one off it, which is how this read
        // 3599 on a windows runner one afternoon. What the test is
        // about is that the size of the skew is in the sentence, so it
        // asks for the size and not for the digits.
        let ahead: u64 = skew
            .detail
            .split_whitespace()
            .find_map(|word| word.parse().ok())
            .unwrap_or_else(|| panic!("the report should carry the size of it: {}", skew.detail));
        assert!(
            (3599..=3600).contains(&ahead),
            "an hour ahead should read as an hour: {}",
            skew.detail
        );
    }

    #[test]
    fn a_manifest_from_the_past_says_only_what_it_can() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let mut manifest = Manifest::new("local", 18);
        manifest.published_unix = Some(now_unix() - 90);
        store
            .put(&TenantLayout::new("local").manifest(), &manifest.to_json())
            .unwrap();
        let skew = clock(&store, "local");
        assert_eq!(skew.status, Status::Ok);
        assert!(
            skew.detail.contains("says nothing about the other way"),
            "an old manifest is not proof of a good clock: {}",
            skew.detail
        );
    }

    #[test]
    fn the_tenant_the_clock_is_read_from_is_the_one_that_was_asked_for() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        let mut manifest = Manifest::new("acme", 18);
        manifest.published_unix = Some(now_unix() + 3600);
        store
            .put(&TenantLayout::new("acme").manifest(), &manifest.to_json())
            .unwrap();
        assert_eq!(clock(&store, "local").status, Status::Skipped);
        assert_eq!(clock(&store, "acme").status, Status::Failed);
    }
}
