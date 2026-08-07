//! Injected store latency for benchmarking.
//!
//! Real S3 answers a ranged GET in 10 to 20 ms and a PUT in 20 to 40
//! ms, while local MinIO answers in well under a millisecond, so a
//! benchmark against MinIO flatters every path whose cost is round
//! trips. [`DelayStore`] sleeps for a configured time inside each
//! operation, which slows the group commit worker, the freshness
//! barrier, and page reads exactly where a distant store would. The
//! numbers it produces are labeled simulated, they stand in for real
//! S3 until credentials exist, they do not replace it.
//!
//! `ZOU_STORE_DELAY=get=15,put=25,list=40` makes [`crate::open_store`]
//! wrap whatever backend the target names. Unset keys default to 0 and
//! unknown keys are an error, a typo must not silently benchmark the
//! wrong thing.

use std::time::Duration;

use crate::cas::{CasError, CasStore, Version};

/// Per operation delays in milliseconds. GET covers ranged reads too,
/// PUT covers every write shape, conditional or not.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DelayConfig {
    pub get_ms: u64,
    pub put_ms: u64,
    pub list_ms: u64,
    pub delete_ms: u64,
}

impl DelayConfig {
    /// Parse `get=15,put=25,list=40,delete=15`. Every key is optional.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let mut cfg = Self::default();
        for part in spec.split(',').filter(|p| !p.is_empty()) {
            let (key, value) = part
                .split_once('=')
                .ok_or_else(|| format!("bad delay entry {part:?}, want key=ms"))?;
            let ms = value
                .parse()
                .map_err(|_| format!("bad delay value {value:?} for {key}"))?;
            match key {
                "get" => cfg.get_ms = ms,
                "put" => cfg.put_ms = ms,
                "list" => cfg.list_ms = ms,
                "delete" => cfg.delete_ms = ms,
                _ => return Err(format!("unknown delay key {key:?}")),
            }
        }
        Ok(cfg)
    }
}

/// A store that adds fixed latency to every call before delegating.
pub struct DelayStore {
    inner: Box<dyn CasStore>,
    config: DelayConfig,
}

impl DelayStore {
    pub fn new(inner: Box<dyn CasStore>, config: DelayConfig) -> Self {
        Self { inner, config }
    }

    fn nap(ms: u64) {
        if ms > 0 {
            std::thread::sleep(Duration::from_millis(ms));
        }
    }
}

impl CasStore for DelayStore {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        Self::nap(self.config.get_ms);
        self.inner.get(key)
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, CasError> {
        Self::nap(self.config.get_ms);
        self.inner.get_range(key, offset, len)
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        Self::nap(self.config.put_ms);
        self.inner.put_if_match(key, data, expected)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        Self::nap(self.config.put_ms);
        self.inner.put(key, data)
    }

    fn delete(&self, key: &str) -> Result<(), CasError> {
        Self::nap(self.config.delete_ms);
        self.inner.delete(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        Self::nap(self.config.list_ms);
        self.inner.list(prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::LocalFsStore;
    use std::time::Instant;

    #[test]
    fn the_spec_parses_and_rejects_typos() {
        assert_eq!(DelayConfig::parse("").unwrap(), DelayConfig::default());
        let cfg = DelayConfig::parse("get=15,put=25,list=40,delete=5").unwrap();
        assert_eq!(cfg.get_ms, 15);
        assert_eq!(cfg.put_ms, 25);
        assert_eq!(cfg.list_ms, 40);
        assert_eq!(cfg.delete_ms, 5);
        assert!(DelayConfig::parse("gte=15").is_err());
        assert!(DelayConfig::parse("get=fast").is_err());
        assert!(DelayConfig::parse("get").is_err());
    }

    #[test]
    fn delays_apply_and_results_pass_through() {
        let dir = tempfile::tempdir().unwrap();
        let store = DelayStore::new(
            Box::new(LocalFsStore::new(dir.path())),
            DelayConfig::parse("put=300").unwrap(),
        );
        let start = Instant::now();
        store.put("k", b"v").unwrap();
        assert!(start.elapsed() >= Duration::from_millis(300));
        // Reads carry no configured delay here and still see the write.
        //
        // The bound is most of the configured delay rather than a small
        // number of milliseconds, because what is being asked is
        // whether the put's sleep leaked into the get, and a read of a
        // file in a temporary directory takes as long as the machine
        // feels like taking. A windows runner sleeping on a 15.6 ms
        // timer under load failed a 30 ms bound here, which said
        // nothing about the code.
        let start = Instant::now();
        assert_eq!(store.get("k").unwrap().unwrap().0, b"v");
        assert!(start.elapsed() < Duration::from_millis(250));
        assert_eq!(store.list("").unwrap(), vec!["k"]);
    }
}
