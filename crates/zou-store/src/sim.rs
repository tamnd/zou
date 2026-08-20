//! Simulated object store behavior for benchmarking, one profile per provider.
//!
//! [`crate::delay::DelayStore`] injects one fixed delay per op kind, which is
//! enough to expose round trip costs but flatters every tail: real S3 has a
//! p99 several times its p50, big objects pay transfer time, and a loaded
//! bucket answers 503 SlowDown now and then. [`SimStore`] models all three.
//! Each operation samples its latency from a per op quantile curve, adds a
//! transfer term for the bytes moved, and occasionally emulates the SlowDown
//! rounds the real backend's retry loop would pay, with the same 100 ms
//! doubling backoff and the same four attempt budget as [`crate::s3`].
//!
//! `ZOU_STORE_SIM=s3-express` makes [`crate::open_store`] wrap whatever
//! backend the target names. The first item names a built in profile or a
//! calibration file written by the zou-bench probe, and the rest override
//! single fields: `ZOU_STORE_SIM=s3-standard,slowdown=0.01,seed=42`. The
//! built in numbers are defaults from public figures and our own probes, a
//! measured calibration file beats them, and every number produced under
//! this wrapper is labeled simulated in the result book, per the rules in
//! the M1b milestone.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::cas::{CasError, CasStore, Version};

/// One operation's latency curve in milliseconds, anchored at the
/// quantiles a probe can actually measure. Sampling interpolates between
/// the anchors, so a profile reproduces its measured percentiles by
/// construction instead of hoping a distribution family fits.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct OpDist {
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

impl OpDist {
    const fn new(p50_ms: f64, p95_ms: f64, p99_ms: f64, max_ms: f64) -> Self {
        Self {
            p50_ms,
            p95_ms,
            p99_ms,
            max_ms,
        }
    }

    fn validate(&self, what: &str) -> Result<(), String> {
        let ok = self.p50_ms >= 0.0
            && self.p95_ms >= self.p50_ms
            && self.p99_ms >= self.p95_ms
            && self.max_ms >= self.p99_ms;
        if ok {
            Ok(())
        } else {
            Err(format!(
                "{what} quantiles must satisfy 0 <= p50 <= p95 <= p99 <= max, got {self:?}"
            ))
        }
    }

    /// The latency at quantile `u` in [0, 1): piecewise linear through
    /// (0, p50/2), (0.5, p50), (0.95, p95), (0.99, p99), (1, max). The
    /// floor at half the median keeps the fast half of requests plausible
    /// without inventing a fifth number nobody measured.
    fn quantile(&self, u: f64) -> f64 {
        let anchors = [
            (0.0, self.p50_ms * 0.5),
            (0.5, self.p50_ms),
            (0.95, self.p95_ms),
            (0.99, self.p99_ms),
            (1.0, self.max_ms),
        ];
        for pair in anchors.windows(2) {
            let (u0, v0) = pair[0];
            let (u1, v1) = pair[1];
            if u <= u1 {
                let t = if u1 > u0 { (u - u0) / (u1 - u0) } else { 1.0 };
                return v0 + (v1 - v0) * t;
            }
        }
        self.max_ms
    }
}

/// A provider's simulated behavior. `mbps` is per request transfer
/// throughput, charged on the bytes a call actually moves, so a 64 MB
/// segment costs its transfer time and a manifest swap costs none.
/// `slowdown` is the per request probability of a 503 SlowDown round.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SimProfile {
    pub name: String,
    pub get: OpDist,
    pub put: OpDist,
    pub list: OpDist,
    pub delete: OpDist,
    #[serde(default = "default_mbps")]
    pub mbps: f64,
    #[serde(default)]
    pub slowdown: f64,
}

fn default_mbps() -> f64 {
    80.0
}

/// Built in defaults per provider. These are starting points from public
/// latency figures and our own MacBook and server3 probes, good enough to
/// rank designs, and the zou-bench probe subcommand replaces them with a
/// calibration file measured against the real endpoint before any number
/// leaves the result book.
fn builtin(name: &str) -> Option<SimProfile> {
    let (get, put, list, delete, mbps, slowdown) = match name {
        "s3-standard" => (
            OpDist::new(18.0, 45.0, 120.0, 400.0),
            OpDist::new(30.0, 80.0, 200.0, 600.0),
            OpDist::new(40.0, 90.0, 200.0, 600.0),
            OpDist::new(25.0, 60.0, 150.0, 500.0),
            80.0,
            0.0005,
        ),
        "s3-express" => (
            OpDist::new(2.5, 5.0, 9.0, 30.0),
            OpDist::new(4.0, 8.0, 15.0, 40.0),
            OpDist::new(6.0, 12.0, 20.0, 50.0),
            OpDist::new(3.0, 6.0, 12.0, 35.0),
            200.0,
            0.0002,
        ),
        "r2" => (
            OpDist::new(25.0, 60.0, 150.0, 500.0),
            OpDist::new(45.0, 110.0, 250.0, 800.0),
            OpDist::new(50.0, 120.0, 250.0, 800.0),
            OpDist::new(35.0, 90.0, 200.0, 700.0),
            60.0,
            0.0005,
        ),
        "gcs" => (
            OpDist::new(20.0, 50.0, 130.0, 450.0),
            OpDist::new(35.0, 90.0, 220.0, 700.0),
            OpDist::new(45.0, 100.0, 220.0, 700.0),
            OpDist::new(30.0, 70.0, 170.0, 550.0),
            80.0,
            0.0005,
        ),
        "b2" => (
            OpDist::new(35.0, 90.0, 250.0, 900.0),
            OpDist::new(60.0, 150.0, 400.0, 1200.0),
            OpDist::new(70.0, 160.0, 400.0, 1200.0),
            OpDist::new(50.0, 120.0, 300.0, 1000.0),
            50.0,
            0.001,
        ),
        "wasabi" => (
            OpDist::new(30.0, 80.0, 220.0, 800.0),
            OpDist::new(50.0, 130.0, 350.0, 1000.0),
            OpDist::new(60.0, 140.0, 350.0, 1000.0),
            OpDist::new(40.0, 100.0, 270.0, 900.0),
            60.0,
            0.001,
        ),
        _ => return None,
    };
    Some(SimProfile {
        name: name.to_string(),
        get,
        put,
        list,
        delete,
        mbps,
        slowdown,
    })
}

/// Names of every built in profile, for error messages and docs.
pub const BUILTIN_PROFILES: &[&str] = &["s3-standard", "s3-express", "r2", "gcs", "b2", "wasabi"];

/// The three places a key can be wrong say the same thing, because from
/// the outside they are the same mistake. Naming the whole grammar
/// rather than the one key beats sending somebody to the source to find
/// out what `put_p99` was made of.
fn unknown_key(key: &str) -> String {
    format!(
        "unknown sim key {key:?}, the keys are slowdown, mbps, seed, and an op and quantile joined by an underscore, where the op is one of get put list delete and the quantile is one of p50 p95 p99 max"
    )
}

/// Parsed `ZOU_STORE_SIM` value: the profile after overrides, plus an
/// optional seed so a run can be replayed with the same latency draws.
#[derive(Debug, Clone, PartialEq)]
pub struct SimConfig {
    pub profile: SimProfile,
    pub seed: Option<u64>,
}

impl SimConfig {
    /// Parse `<profile>[,key=value...]`. The profile is a built in name or
    /// a path to a calibration json, overrides are `slowdown`, `mbps`,
    /// `seed`, and `<op>_<quantile>` like `put_p99=150`. Unknown keys are
    /// an error, a typo must not silently benchmark the wrong provider.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let mut parts = spec.split(',').filter(|p| !p.is_empty());
        let head = parts.next().ok_or("empty ZOU_STORE_SIM")?;
        let mut profile = if head.contains('/') || head.contains('\\') || head.ends_with(".json") {
            let data = std::fs::read(head)
                .map_err(|e| format!("cannot read calibration file {head}: {e}"))?;
            serde_json::from_slice::<SimProfile>(&data)
                .map_err(|e| {
                    format!(
                        "bad calibration file {head}: {e}, want the json a calibration run writes out, which is one object with a slowdown, an mbps and a get put list delete each holding p50 p95 p99 max"
                    )
                })?
        } else {
            builtin(head).ok_or_else(|| {
                format!(
                    "unknown sim profile {head:?}, builtins are {}",
                    BUILTIN_PROFILES.join(", ")
                )
            })?
        };
        let mut seed = None;
        for part in parts {
            let (key, value) = part
                .split_once('=')
                .ok_or_else(|| format!("bad sim entry {part:?}, want key=value"))?;
            let num = || -> Result<f64, String> {
                value
                    .parse()
                    .map_err(|_| format!("bad sim value {value:?} for {key}, write a number"))
            };
            match key {
                "slowdown" => {
                    profile.slowdown = num()?;
                    if !(0.0..=1.0).contains(&profile.slowdown) {
                        return Err(format!(
                            "bad sim value {value:?} for slowdown, write a probability from 0 to 1"
                        ));
                    }
                }
                "mbps" => profile.mbps = num()?,
                "seed" => {
                    seed = Some(value.parse().map_err(|_| {
                        format!("bad sim value {value:?} for seed, write a whole number")
                    })?)
                }
                _ => {
                    let (op, q) = key.split_once('_').ok_or_else(|| unknown_key(key))?;
                    let dist = match op {
                        "get" => &mut profile.get,
                        "put" => &mut profile.put,
                        "list" => &mut profile.list,
                        "delete" => &mut profile.delete,
                        _ => return Err(unknown_key(key)),
                    };
                    match q {
                        "p50" => dist.p50_ms = num()?,
                        "p95" => dist.p95_ms = num()?,
                        "p99" => dist.p99_ms = num()?,
                        "max" => dist.max_ms = num()?,
                        _ => return Err(unknown_key(key)),
                    }
                }
            }
        }
        if profile.mbps <= 0.0 {
            return Err(format!(
                "bad sim value {} for mbps, write a number above zero",
                profile.mbps
            ));
        }
        for (dist, what) in [
            (&profile.get, "get"),
            (&profile.put, "put"),
            (&profile.list, "list"),
            (&profile.delete, "delete"),
        ] {
            dist.validate(what)?;
        }
        Ok(Self { profile, seed })
    }
}

/// A store that samples per provider latency, transfer time, and SlowDown
/// rounds before delegating. Results and errors pass through untouched,
/// only time is added, so correctness under the simulator is correctness.
pub struct SimStore {
    inner: Box<dyn CasStore>,
    profile: SimProfile,
    state: AtomicU64,
    slowdowns: AtomicU64,
}

/// Attempt budget for a SlowDown burst, mirroring [`crate::s3`]: four
/// attempts total, backoff starting at 100 ms and doubling.
const MAX_ATTEMPTS: u32 = 4;
const RETRY_BASE_MS: u64 = 100;

impl SimStore {
    pub fn new(inner: Box<dyn CasStore>, config: SimConfig) -> Self {
        let seed = config.seed.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0x9e3779b97f4a7c15)
        });
        Self {
            inner,
            profile: config.profile,
            state: AtomicU64::new(seed),
            slowdowns: AtomicU64::new(0),
        }
    }

    /// How many SlowDown rounds this store has emulated, for tests and
    /// for anyone debugging a suspicious tail.
    pub fn slowdowns(&self) -> u64 {
        self.slowdowns.load(Ordering::Relaxed)
    }

    /// splitmix64 over an atomic state: cheap, seedable, and good enough
    /// for latency draws, which need no cryptography. A dependency on a
    /// rand crate is not worth five lines.
    fn next_u64(&self) -> u64 {
        let s = self
            .state
            .fetch_add(0x9e3779b97f4a7c15, Ordering::Relaxed)
            .wrapping_add(0x9e3779b97f4a7c15);
        let mut z = s;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    fn next_unit(&self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn nap_ms(ms: f64) {
        if ms > 0.0 {
            std::thread::sleep(Duration::from_secs_f64(ms / 1000.0));
        }
    }

    /// Sleep one sampled request latency, then any SlowDown rounds the
    /// dice call for. Every extra round pays a fresh latency draw plus
    /// the backoff step the real retry loop would sleep. Four throttled
    /// attempts in a row fail the op the way the real backend gives up,
    /// which honest configs will never see and torture configs test.
    fn op(&self, key: &str, dist: &OpDist) -> Result<(), CasError> {
        for attempt in 1..=MAX_ATTEMPTS {
            Self::nap_ms(dist.quantile(self.next_unit()));
            if self.next_unit() >= self.profile.slowdown {
                return Ok(());
            }
            self.slowdowns.fetch_add(1, Ordering::Relaxed);
            if attempt == MAX_ATTEMPTS {
                break;
            }
            let wait = RETRY_BASE_MS * 2u64.pow(attempt - 1);
            log::debug!(
                "sim {}: 503 SlowDown on {key}, retrying in {wait} ms",
                self.profile.name
            );
            Self::nap_ms(wait as f64);
        }
        Err(CasError::Io {
            key: key.to_string(),
            source: std::io::Error::other("simulated 503 SlowDown, attempts exhausted"),
        })
    }

    fn transfer(&self, bytes: usize) {
        Self::nap_ms(bytes as f64 / (self.profile.mbps * 1_000_000.0) * 1000.0);
    }
}

impl CasStore for SimStore {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        self.op(key, &self.profile.get)?;
        let got = self.inner.get(key)?;
        if let Some((data, _)) = &got {
            self.transfer(data.len());
        }
        Ok(got)
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, CasError> {
        self.op(key, &self.profile.get)?;
        let got = self.inner.get_range(key, offset, len)?;
        if let Some(data) = &got {
            self.transfer(data.len());
        }
        Ok(got)
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        self.op(key, &self.profile.put)?;
        self.transfer(data.len());
        self.inner.put_if_match(key, data, expected)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        self.op(key, &self.profile.put)?;
        self.transfer(data.len());
        self.inner.put(key, data)
    }

    /// Signing a url is arithmetic against no backend, so there is
    /// nothing here to fail or slow down.
    fn presigned_get(
        &self,
        key: &str,
        ttl: Duration,
        response: &[(&str, &str)],
    ) -> Result<Option<String>, CasError> {
        self.inner.presigned_get(key, ttl, response)
    }

    fn delete(&self, key: &str) -> Result<(), CasError> {
        self.op(key, &self.profile.delete)?;
        self.inner.delete(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        self.op(prefix, &self.profile.list)?;
        self.inner.list(prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::LocalFsStore;
    use std::time::Instant;

    fn store_with(spec: &str) -> (tempfile::TempDir, SimStore) {
        let dir = tempfile::tempdir().unwrap();
        let sim = SimStore::new(
            Box::new(LocalFsStore::new(dir.path())),
            SimConfig::parse(spec).unwrap(),
        );
        (dir, sim)
    }

    #[test]
    fn every_builtin_parses_and_validates() {
        for name in BUILTIN_PROFILES {
            let cfg = SimConfig::parse(name).unwrap();
            assert_eq!(cfg.profile.name, *name);
            assert!(cfg.seed.is_none());
        }
        assert!(SimConfig::parse("s3-clasic").is_err());
        assert!(SimConfig::parse("").is_err());
    }

    #[test]
    fn overrides_apply_and_typos_are_rejected() {
        let cfg = SimConfig::parse("s3-standard,put_p99=150,slowdown=0.01,mbps=40,seed=7").unwrap();
        assert_eq!(cfg.profile.put.p99_ms, 150.0);
        assert_eq!(cfg.profile.slowdown, 0.01);
        assert_eq!(cfg.profile.mbps, 40.0);
        assert_eq!(cfg.seed, Some(7));
        assert!(SimConfig::parse("s3-standard,put_p98=1").is_err());
        assert!(SimConfig::parse("s3-standard,pit_p99=1").is_err());
        assert!(SimConfig::parse("s3-standard,slowdown=2").is_err());
        assert!(SimConfig::parse("s3-standard,mbps=0").is_err());
        assert!(SimConfig::parse("s3-standard,put_p99").is_err());
        // Overrides must not break quantile ordering.
        assert!(SimConfig::parse("s3-standard,put_max=1").is_err());
    }

    #[test]
    fn a_calibration_file_loads_and_beats_builtins() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("probe-minio.json");
        let profile = SimProfile {
            name: "minio-server3".into(),
            get: OpDist::new(1.0, 2.0, 4.0, 10.0),
            put: OpDist::new(2.0, 3.0, 6.0, 15.0),
            list: OpDist::new(2.0, 4.0, 8.0, 20.0),
            delete: OpDist::new(1.0, 2.0, 4.0, 10.0),
            mbps: 500.0,
            slowdown: 0.0,
        };
        std::fs::write(&path, serde_json::to_vec(&profile).unwrap()).unwrap();
        let cfg = SimConfig::parse(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.profile, profile);
    }

    #[test]
    fn quantiles_hit_their_anchors_and_stay_monotone() {
        let d = OpDist::new(10.0, 20.0, 40.0, 100.0);
        assert_eq!(d.quantile(0.0), 5.0);
        assert_eq!(d.quantile(0.5), 10.0);
        assert_eq!(d.quantile(0.95), 20.0);
        assert_eq!(d.quantile(0.99), 40.0);
        assert_eq!(d.quantile(1.0), 100.0);
        let mut last = 0.0;
        for i in 0..=1000 {
            let v = d.quantile(i as f64 / 1000.0);
            assert!(v >= last, "quantile must not decrease");
            last = v;
        }
    }

    #[test]
    fn seeded_draws_replay_and_land_inside_the_curve() {
        let (_dir, a) = store_with("s3-express,seed=42,slowdown=0");
        let (_dir2, b) = store_with("s3-express,seed=42,slowdown=0");
        let draws_a: Vec<u64> = (0..64).map(|_| a.next_u64()).collect();
        let draws_b: Vec<u64> = (0..64).map(|_| b.next_u64()).collect();
        assert_eq!(draws_a, draws_b);
        for _ in 0..1000 {
            let u = a.next_unit();
            assert!((0.0..1.0).contains(&u));
        }
    }

    #[test]
    fn latency_is_paid_and_results_pass_through() {
        // A tight profile keeps the test fast while proving sleeps happen:
        // p50 8 ms means the median put waits at least a few ms.
        let (_dir, sim) = store_with(
            "s3-express,seed=1,slowdown=0,put_p50=8,put_p95=9,put_p99=10,put_max=11,mbps=100000",
        );
        let start = Instant::now();
        sim.put("k", b"v").unwrap();
        assert!(start.elapsed() >= Duration::from_millis(4));
        assert_eq!(sim.get("k").unwrap().unwrap().0, b"v");
        assert_eq!(sim.list("").unwrap(), vec!["k"]);
        assert_eq!(sim.slowdowns(), 0);
    }

    #[test]
    fn big_bodies_pay_transfer_time() {
        let (_dir, sim) = store_with(
            "s3-express,seed=1,slowdown=0,mbps=10,put_p50=0,put_p95=0,put_p99=0,put_max=0",
        );
        // 1 MB at 10 MB/s is 100 ms of transfer on top of zero latency.
        let body = vec![0u8; 1_000_000];
        let start = Instant::now();
        sim.put("big", &body).unwrap();
        assert!(start.elapsed() >= Duration::from_millis(95));
    }

    #[test]
    fn slowdown_rounds_count_and_exhaustion_fails_like_the_real_backend() {
        let (_dir, sim) =
            store_with("s3-express,seed=3,slowdown=1,get_p50=0,get_p95=0,get_p99=0,get_max=0");
        let start = Instant::now();
        let err = sim.get("k").unwrap_err();
        assert!(matches!(err, CasError::Io { .. }), "got {err:?}");
        assert_eq!(sim.slowdowns(), MAX_ATTEMPTS as u64);
        // Three backoff sleeps: 100 + 200 + 400 ms.
        assert!(start.elapsed() >= Duration::from_millis(690));
    }
}
