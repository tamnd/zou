//! The CAS contract run over every backend: local filesystem, the
//! prefix wrapper, .zou single file, SQLite when the feature is on, and
//! any S3 compatible endpoint named by the environment. The properties
//! themselves live in tests/common/mod.rs.

mod common;

use std::sync::Arc;

use common::run_contract;
use zou_store::{LocalFsStore, PrefixStore, ZouFileStore};

#[test]
fn local_fs_backend_passes_the_contract() {
    let dir = tempfile::tempdir().unwrap();
    run_contract(Arc::new(LocalFsStore::new(dir.path())));
}

#[test]
fn zou_file_backend_passes_the_contract() {
    let dir = tempfile::tempdir().unwrap();
    run_contract(Arc::new(
        ZouFileStore::open(dir.path().join("contract.zou")).unwrap(),
    ));
}

/// The wrapper `s3://bucket/prefix` targets go through must preserve the
/// contract over any backend, so run it over the local one too.
#[test]
fn a_prefixed_store_passes_the_contract() {
    let dir = tempfile::tempdir().unwrap();
    run_contract(Arc::new(PrefixStore::new(
        Box::new(LocalFsStore::new(dir.path())),
        "nested/store",
    )));
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_backend_passes_the_contract() {
    let dir = tempfile::tempdir().unwrap();
    run_contract(Arc::new(
        zou_store::SqliteStore::open(dir.path().join("contract.db")).unwrap(),
    ));
}

/// Runs against any S3 compatible endpoint, MinIO in CI. Skips unless
/// ZOU_S3_TEST_ENDPOINT is set so plain `cargo test` stays offline.
#[cfg(feature = "s3")]
#[test]
fn s3_backend_passes_the_contract() {
    let Ok(endpoint) = std::env::var("ZOU_S3_TEST_ENDPOINT") else {
        eprintln!("skipping: ZOU_S3_TEST_ENDPOINT not set");
        return;
    };
    let var = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set"));
    let dialect = match std::env::var("ZOU_S3_TEST_DIALECT").as_deref() {
        Ok("gcs") => zou_store::Dialect::Gcs,
        _ => zou_store::Dialect::S3,
    };
    let store = zou_store::S3Store::new(zou_store::S3Config {
        endpoint,
        region: std::env::var("ZOU_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into()),
        bucket: var("ZOU_S3_TEST_BUCKET"),
        access_key: var("ZOU_S3_TEST_ACCESS_KEY"),
        secret_key: var("ZOU_S3_TEST_SECRET_KEY"),
        dialect,
    });
    // A per-run nonce prefix keeps the contract's fixed key names from
    // colliding across runs against a long lived bucket.
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    run_contract(Arc::new(PrefixStore::new(
        Box::new(store),
        &format!("contract-runs/{nonce:x}"),
    )));
}
