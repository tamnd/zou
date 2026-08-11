//! Open a store from a target string.
//!
//! A plain path opens the local filesystem backend. `s3://bucket/prefix`
//! opens the S3 backend with connection settings from the environment,
//! and `gs://bucket/prefix` is the same wire client speaking the GCS
//! dialect. The prefix scopes every key through [`PrefixStore`], so
//! several stores can share one bucket.
//!
//! Environment for the object store schemes:
//! - `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, required
//! - `ZOU_S3_ENDPOINT`, defaults to `https://s3.<region>.amazonaws.com`
//!   for s3 and `https://storage.googleapis.com` for gs
//! - `ZOU_S3_REGION`, falls back to `AWS_REGION`, then `us-east-1`
//!
//! Three wrappers apply to every backend: `ZOU_STORE_SIM` simulates a
//! provider's latency curve, transfer time, and SlowDown behavior, see
//! [`crate::sim`], `ZOU_STORE_DELAY` injects fixed per op delays, see
//! [`crate::delay`], and `ZOU_STORE_STATS` counts ops into a shared
//! counter file, see [`crate::stats`]. SIM and DELAY answer the same
//! question two ways, so setting both is an error.

use std::sync::Arc;

use crate::cas::{CasError, CasStore, LocalFsStore, Version};
use crate::delay::{DelayConfig, DelayStore};
use crate::sim::{SimConfig, SimStore};

/// A store that scopes every key under a fixed prefix, so one bucket can
/// hold many stores. Callers see their own key space: keys are prefixed
/// on the way in and list results come back stripped.
pub struct PrefixStore {
    inner: Arc<dyn CasStore>,
    prefix: String,
}

impl PrefixStore {
    /// Wrap `inner` under `prefix`, with or without a trailing slash.
    pub fn new(inner: Box<dyn CasStore>, prefix: &str) -> Self {
        Self::over(Arc::from(inner), prefix)
    }

    /// The same, for a store the caller already shares. Opening one
    /// backend and scoping it several ways is how one process reaches
    /// several key spaces without paying for several clients.
    pub fn over(inner: Arc<dyn CasStore>, prefix: &str) -> Self {
        let mut prefix = prefix.trim_matches('/').to_string();
        prefix.push('/');
        Self { inner, prefix }
    }

    fn key(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }
}

impl CasStore for PrefixStore {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        self.inner.get(&self.key(key))
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, CasError> {
        self.inner.get_range(&self.key(key), offset, len)
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        self.inner.put_if_match(&self.key(key), data, expected)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        self.inner.put(&self.key(key), data)
    }

    fn presigned_get(
        &self,
        key: &str,
        ttl: std::time::Duration,
        response: &[(&str, &str)],
    ) -> Result<Option<String>, CasError> {
        self.inner.presigned_get(&self.key(key), ttl, response)
    }

    fn delete(&self, key: &str) -> Result<(), CasError> {
        self.inner.delete(&self.key(key))
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        let keys = self.inner.list(&self.key(prefix))?;
        Ok(keys
            .into_iter()
            .filter_map(|k| k.strip_prefix(&self.prefix).map(str::to_string))
            .collect())
    }
}

/// What a target string names. Parsing only, no environment access, so
/// the rules are testable without a live endpoint.
#[derive(Debug, PartialEq, Eq)]
enum Parsed<'a> {
    Local(&'a str),
    Sqlite(&'a str),
    Remote {
        scheme: &'a str,
        bucket: &'a str,
        prefix: &'a str,
    },
}

fn parse(target: &str) -> Result<Parsed<'_>, String> {
    let Some((scheme, rest)) = target.split_once("://") else {
        return Ok(Parsed::Local(target));
    };
    if scheme == "sqlite" {
        if rest.is_empty() {
            return Err(format!("{target} names no database file"));
        }
        return Ok(Parsed::Sqlite(rest));
    }
    if scheme != "s3" && scheme != "gs" {
        return Err(format!(
            "unsupported store scheme {scheme}://, use a path, sqlite://, s3://, or gs://"
        ));
    }
    let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        return Err(format!("{target} names no bucket"));
    }
    Ok(Parsed::Remote {
        scheme,
        bucket,
        prefix: prefix.trim_matches('/'),
    })
}

/// Open the backend a target string names: a filesystem directory, or an
/// `s3://bucket/prefix` style URL configured from the environment.
/// `ZOU_STORE_SIM` wraps the result in [`SimStore`] for a full provider
/// latency profile, `ZOU_STORE_DELAY` in [`DelayStore`] for fixed per op
/// delays, see [`crate::sim`] and [`crate::delay`].
pub fn open_store(target: &str) -> Result<Box<dyn CasStore>, String> {
    let store: Box<dyn CasStore> = match parse(target)? {
        Parsed::Local(path) if path.ends_with(".zou") => {
            Box::new(crate::zoufile::ZouFileStore::open(path)?)
        }
        Parsed::Local(path)
            if path.ends_with(".db") || path.ends_with(".sqlite") || path.ends_with(".sqlite3") =>
        {
            open_sqlite(path)?
        }
        Parsed::Local(path) => Box::new(LocalFsStore::new(path)),
        Parsed::Sqlite(path) => open_sqlite(path)?,
        Parsed::Remote {
            scheme,
            bucket,
            prefix,
        } => {
            let store = open_remote(scheme, bucket)?;
            if prefix.is_empty() {
                store
            } else {
                Box::new(PrefixStore::new(store, prefix))
            }
        }
    };
    let sim_spec = std::env::var("ZOU_STORE_SIM")
        .ok()
        .filter(|s| !s.is_empty());
    let delay_spec = std::env::var("ZOU_STORE_DELAY")
        .ok()
        .filter(|s| !s.is_empty());
    if sim_spec.is_some() && delay_spec.is_some() {
        return Err("ZOU_STORE_SIM and ZOU_STORE_DELAY are both set, pick one simulation".into());
    }
    let store: Box<dyn CasStore> = if let Some(spec) = sim_spec {
        let config = SimConfig::parse(&spec).map_err(|e| format!("ZOU_STORE_SIM: {e}"))?;
        Box::new(SimStore::new(store, config))
    } else if let Some(spec) = delay_spec {
        let config = DelayConfig::parse(&spec).map_err(|e| format!("ZOU_STORE_DELAY: {e}"))?;
        Box::new(DelayStore::new(store, config))
    } else {
        store
    };
    // Counters wrap outermost, after any simulated delay, so the
    // latency they record is the latency callers actually paid.
    match std::env::var("ZOU_STORE_STATS") {
        Ok(path) if !path.is_empty() => {
            let stats = crate::stats::StatsStore::new(store, std::path::Path::new(&path))
                .map_err(|e| format!("ZOU_STORE_STATS {path}: {e}"))?;
            Ok(Box::new(stats))
        }
        _ => Ok(store),
    }
}

#[cfg(feature = "s3")]
fn open_remote(scheme: &str, bucket: &str) -> Result<Box<dyn CasStore>, String> {
    use crate::s3::{Dialect, S3Config, S3Store};
    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    let dialect = if scheme == "gs" {
        Dialect::Gcs
    } else {
        Dialect::S3
    };
    let region = var("ZOU_S3_REGION")
        .or_else(|| var("AWS_REGION"))
        .unwrap_or_else(|| "us-east-1".into());
    let endpoint = var("ZOU_S3_ENDPOINT").unwrap_or_else(|| match dialect {
        Dialect::S3 => format!("https://s3.{region}.amazonaws.com"),
        Dialect::Gcs => "https://storage.googleapis.com".into(),
    });
    let need = |name: &str| {
        var(name).ok_or_else(|| format!("{scheme}:// needs {name} in the environment"))
    };
    Ok(Box::new(S3Store::new(S3Config {
        endpoint,
        region,
        bucket: bucket.to_string(),
        access_key: need("AWS_ACCESS_KEY_ID")?,
        secret_key: need("AWS_SECRET_ACCESS_KEY")?,
        // Set wherever a role hands out the credentials rather than a
        // person: a Lambda function, an ECS task, an EC2 instance
        // profile. Absent for a static key pair, which is most laptops
        // and every MinIO.
        session: var("AWS_SESSION_TOKEN"),
        dialect,
    })))
}

#[cfg(not(feature = "s3"))]
fn open_remote(scheme: &str, _bucket: &str) -> Result<Box<dyn CasStore>, String> {
    Err(format!(
        "{scheme}:// needs the s3 feature, this build has only the local backend"
    ))
}

#[cfg(feature = "sqlite")]
fn open_sqlite(path: &str) -> Result<Box<dyn CasStore>, String> {
    Ok(Box::new(crate::sqlite::SqliteStore::open(path)?))
}

#[cfg(not(feature = "sqlite"))]
fn open_sqlite(path: &str) -> Result<Box<dyn CasStore>, String> {
    Err(format!(
        "{path} needs the sqlite feature, which this build lacks"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_paths_stay_local_and_urls_split_into_bucket_and_prefix() {
        assert_eq!(
            parse("/var/lib/zou").unwrap(),
            Parsed::Local("/var/lib/zou")
        );
        assert_eq!(
            parse("C:\\zou\\store").unwrap(),
            Parsed::Local("C:\\zou\\store")
        );
        assert_eq!(
            parse("s3://bucket").unwrap(),
            Parsed::Remote {
                scheme: "s3",
                bucket: "bucket",
                prefix: ""
            }
        );
        assert_eq!(
            parse("s3://bucket/a/b/").unwrap(),
            Parsed::Remote {
                scheme: "s3",
                bucket: "bucket",
                prefix: "a/b"
            }
        );
        assert_eq!(
            parse("gs://bucket/store").unwrap(),
            Parsed::Remote {
                scheme: "gs",
                bucket: "bucket",
                prefix: "store"
            }
        );
        assert_eq!(
            parse("sqlite:///var/lib/zou.db").unwrap(),
            Parsed::Sqlite("/var/lib/zou.db")
        );
        assert!(parse("s3://").is_err());
        assert!(parse("s3:///prefix-only").is_err());
        assert!(parse("sqlite://").is_err());
        assert!(parse("http://host/bucket").is_err());
    }

    #[test]
    fn a_prefix_store_scopes_keys_and_hides_its_neighbors() {
        let dir = tempfile::tempdir().unwrap();
        let a = PrefixStore::new(Box::new(LocalFsStore::new(dir.path())), "tenant-a");
        let b = PrefixStore::new(Box::new(LocalFsStore::new(dir.path())), "tenant-b/");
        a.put("wal/seg1", b"aaa").unwrap();
        b.put("wal/seg1", b"bbb").unwrap();
        assert_eq!(a.get("wal/seg1").unwrap().unwrap().0, b"aaa");
        assert_eq!(b.get("wal/seg1").unwrap().unwrap().0, b"bbb");
        assert_eq!(a.list("wal/").unwrap(), vec!["wal/seg1"]);
        let raw = LocalFsStore::new(dir.path());
        assert_eq!(
            raw.list("").unwrap(),
            vec!["tenant-a/wal/seg1", "tenant-b/wal/seg1"]
        );
        a.delete("wal/seg1").unwrap();
        assert!(a.get("wal/seg1").unwrap().is_none());
        assert_eq!(b.get("wal/seg1").unwrap().unwrap().0, b"bbb");
    }

    #[test]
    fn open_store_returns_a_working_local_backend() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path().to_str().unwrap()).unwrap();
        store.put("k", b"v").unwrap();
        assert_eq!(store.get("k").unwrap().unwrap().0, b"v");
    }
}
