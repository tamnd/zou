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
            let ms = value.parse().map_err(|_| {
                format!("bad delay value {value:?} for {key}, write a whole number of milliseconds")
            })?;
            match key {
                "get" => cfg.get_ms = ms,
                "put" => cfg.put_ms = ms,
                "list" => cfg.list_ms = ms,
                "delete" => cfg.delete_ms = ms,
                _ => {
                    return Err(format!(
                        "unknown delay key {key:?}, the keys are get put list delete"
                    ));
                }
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

    /// Signing a url is arithmetic against no backend, so there is
    /// nothing here to delay.
    fn presigned_get(
        &self,
        key: &str,
        ttl: Duration,
        response: &[(&str, &str)],
    ) -> Result<Option<String>, CasError> {
        self.inner.presigned_get(key, ttl, response)
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

    /// A store whose only configured delay is the one named, so that
    /// every other operation on it is wired to a zero.
    fn only(dir: &std::path::Path, spec: &str) -> DelayStore {
        DelayStore::new(
            Box::new(LocalFsStore::new(dir)),
            DelayConfig::parse(spec).unwrap(),
        )
    }

    /// Each operation sleeps for its own configured time and no other.
    ///
    /// Every bound here is a lower one, and that is the point. An upper
    /// bound on how long a call took measures the machine: a windows
    /// runner sleeping on a 15.6 ms timer failed a 30 ms bound here
    /// once, the bound was raised to 250 ms, and a loaded runner failed
    /// that too. Neither failure said anything about the code, and the
    /// next number would have been the third guess at how slow a shared
    /// box gets.
    ///
    /// A lower bound cannot flake, since a sleep of n never returns
    /// early, and asking it of one store per operation says the same
    /// thing the upper bound was reaching for. The bug worth catching
    /// is a nap wired to the wrong field, and on a store where every
    /// other field is zero, a get that naps `put_ms` naps for nothing
    /// and fails its own bound.
    #[test]
    fn every_operation_sleeps_for_its_own_delay() {
        let dir = tempfile::tempdir().unwrap();
        let nap = Duration::from_millis(120);

        let store = only(dir.path(), "put=120");
        let start = Instant::now();
        store.put("k", b"v").unwrap();
        assert!(start.elapsed() >= nap, "put did not sleep for put_ms");

        let store = only(dir.path(), "get=120");
        let start = Instant::now();
        assert_eq!(store.get("k").unwrap().unwrap().0, b"v");
        assert!(start.elapsed() >= nap, "get did not sleep for get_ms");

        let store = only(dir.path(), "list=120");
        let start = Instant::now();
        assert_eq!(store.list("").unwrap(), vec!["k"]);
        assert!(start.elapsed() >= nap, "list did not sleep for list_ms");

        let store = only(dir.path(), "delete=120");
        let start = Instant::now();
        store.delete("k").unwrap();
        assert!(start.elapsed() >= nap, "delete did not sleep for delete_ms");
        assert!(
            store.get("k").unwrap().is_none(),
            "the delete passed through"
        );
    }
}
