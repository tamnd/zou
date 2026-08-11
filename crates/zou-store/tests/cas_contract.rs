//! The CAS contract run over every backend: local filesystem, the
//! prefix wrapper, .zou single file, SQLite when the feature is on, and
//! any S3 compatible endpoint named by the environment. The properties
//! themselves live in tests/common/mod.rs.

mod common;

use std::sync::Arc;

use common::run_contract;
#[cfg(feature = "s3")]
use zou_store::CasStore;
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

/// The endpoint the environment names, or `None` so a plain `cargo
/// test` stays offline.
#[cfg(feature = "s3")]
fn s3_from_env() -> Option<zou_store::S3Config> {
    let Ok(endpoint) = std::env::var("ZOU_S3_TEST_ENDPOINT") else {
        eprintln!("skipping: ZOU_S3_TEST_ENDPOINT not set");
        return None;
    };
    let var = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set"));
    Some(zou_store::S3Config {
        endpoint,
        region: std::env::var("ZOU_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into()),
        bucket: var("ZOU_S3_TEST_BUCKET"),
        access_key: var("ZOU_S3_TEST_ACCESS_KEY"),
        secret_key: var("ZOU_S3_TEST_SECRET_KEY"),
        session: std::env::var("ZOU_S3_TEST_SESSION_TOKEN")
            .ok()
            .filter(|v| !v.is_empty()),
        dialect: match std::env::var("ZOU_S3_TEST_DIALECT").as_deref() {
            Ok("gcs") => zou_store::Dialect::Gcs,
            _ => zou_store::Dialect::S3,
        },
    })
}

/// A prefix nothing else in this run writes under, so the contract's
/// fixed key names cannot collide across runs against a long lived
/// bucket.
#[cfg(feature = "s3")]
fn nonce(what: &str) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{what}/{now:x}")
}

/// Runs against any S3 compatible endpoint, MinIO in CI.
#[cfg(feature = "s3")]
#[test]
fn s3_backend_passes_the_contract() {
    let Some(cfg) = s3_from_env() else { return };
    run_contract(Arc::new(PrefixStore::new(
        Box::new(zou_store::S3Store::new(cfg)),
        &nonce("contract-runs"),
    )));
}

/// A presigned url is the one thing in the trait that cannot be checked
/// by calling the trait again: what it says is only true if a client
/// that has never heard of zou can read the bytes with it. So this puts
/// an object, signs a url for it, and fetches it with a plain http
/// client carrying no credentials of its own.
#[cfg(feature = "s3")]
#[test]
fn a_presigned_url_reads_the_object_from_the_backend() {
    let Some(cfg) = s3_from_env() else { return };
    let store = PrefixStore::new(
        Box::new(zou_store::S3Store::new(cfg)),
        &nonce("presign-runs"),
    );
    let bytes = b"read me without asking zou".to_vec();
    store.put("an-object", &bytes).unwrap();

    // http_status_as_error off, because a refusal is an answer this test
    // wants to read rather than a transport failure.
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    let ttl = std::time::Duration::from_secs(60);
    let url = store
        .presigned_get(
            "an-object",
            ttl,
            &[
                ("response-content-type", "text/plain"),
                (
                    "response-content-disposition",
                    "attachment; filename=\"a b.txt\"",
                ),
            ],
        )
        .unwrap()
        .expect("an S3 backend can name its objects with a url");

    let mut answer = agent.get(&url).call().unwrap();
    assert_eq!(answer.status().as_u16(), 200, "url was {url}");
    assert_eq!(
        answer.headers().get("content-type").unwrap(),
        "text/plain",
        "the type is signed into the url, since after the redirect nobody is left to set it"
    );
    assert_eq!(
        answer.headers().get("content-disposition").unwrap(),
        "attachment; filename=\"a b.txt\"",
        "and so is the download name"
    );
    assert_eq!(answer.body_mut().read_to_vec().unwrap(), bytes);

    // What a url is worth depends entirely on it being unforgeable, so
    // check that editing one breaks it rather than trusting the signer.
    let edited = url.replace(
        "response-content-type=text%2Fplain",
        "response-content-type=text%2Fhtml",
    );
    assert_ne!(
        edited, url,
        "the override is in the query or this asserts nothing"
    );
    let refused = agent.get(&edited).call().unwrap().status().as_u16();
    assert!(
        (400..500).contains(&refused),
        "a url edited into saying something else must be refused, got {refused}"
    );

    let signature = url.rsplit_once("X-Amz-Signature=").unwrap().1;
    let tampered = url.replace(signature, &"0".repeat(signature.len()));
    let refused = agent.get(&tampered).call().unwrap().status().as_u16();
    assert!(
        (400..500).contains(&refused),
        "a url with a made up signature must be refused, got {refused}"
    );
}
