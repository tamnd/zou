//! The fault injection matrix from the design doc, run against a real
//! S3 compatible endpoint through the zou-chaos proxy. Skips unless
//! ZOU_S3_TEST_ENDPOINT is set, the same gate as the plain contract
//! run, and requires an http endpoint since the proxy speaks plain TCP.

#![cfg(feature = "s3")]

mod common;

use std::sync::Arc;

use common::run_contract_expecting_transient_errors;
use zou_chaos::{ChaosConfig, spawn};
use zou_store::{CasError, CasStore, PrefixStore, S3Config, S3Store};

/// The MinIO endpoint as host:port for the proxy, or None to skip.
fn upstream() -> Option<String> {
    let endpoint = std::env::var("ZOU_S3_TEST_ENDPOINT").ok()?;
    let Some(hostport) = endpoint.strip_prefix("http://") else {
        eprintln!("skipping: the chaos proxy needs an http:// test endpoint");
        return None;
    };
    Some(hostport.trim_end_matches('/').to_string())
}

fn store_at(endpoint: String) -> S3Store {
    let var = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set"));
    S3Store::new(S3Config {
        endpoint,
        region: std::env::var("ZOU_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into()),
        bucket: var("ZOU_S3_TEST_BUCKET"),
        access_key: var("ZOU_S3_TEST_ACCESS_KEY"),
        secret_key: var("ZOU_S3_TEST_SECRET_KEY"),
        dialect: zou_store::Dialect::S3,
    })
}

fn nonce() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// Every 5th answer is a 503 and every 7th request stalls, and the full
/// CAS contract must still hold, including the concurrent swap counter.
/// This is the "object store errors: bounded retries with backoff,
/// commits stall rather than lie" row of the failure matrix.
#[test]
fn the_contract_holds_through_injected_503s_and_latency_spikes() {
    let Some(upstream) = upstream() else {
        eprintln!("skipping: ZOU_S3_TEST_ENDPOINT not set");
        return;
    };
    let proxy = spawn(
        "127.0.0.1:0",
        ChaosConfig {
            upstream,
            error_every: 5,
            delay_every: 7,
            delay_ms: 50,
            truncate_put_every: 0,
        },
    )
    .unwrap();
    let store = store_at(format!("http://{}", proxy.addr()));
    run_contract_expecting_transient_errors(Arc::new(PrefixStore::new(
        Box::new(store),
        &format!("chaos-runs/{:x}", nonce()),
    )));
}

/// A PUT cut off halfway must surface as a hard error to the caller and
/// leave the previous object untouched at the endpoint: no partial
/// bytes, no torn version. The partial upload row of the matrix.
#[test]
fn a_truncated_put_errors_and_leaves_the_old_object_intact() {
    let Some(upstream) = upstream() else {
        eprintln!("skipping: ZOU_S3_TEST_ENDPOINT not set");
        return;
    };
    let proxy = spawn(
        "127.0.0.1:0",
        ChaosConfig {
            upstream: upstream.clone(),
            error_every: 0,
            delay_every: 0,
            delay_ms: 0,
            truncate_put_every: 2,
        },
    )
    .unwrap();
    let through_proxy = store_at(format!("http://{}", proxy.addr()));
    // Verification reads bypass the proxy so they see exactly what the
    // endpoint committed.
    let direct = store_at(format!("http://{upstream}"));
    let key = format!("chaos-runs/{:x}/torn-put", nonce());

    let old = vec![0xAA_u8; 128 * 1024];
    let stored = vec![0xBB_u8; 256 * 1024];
    through_proxy.put(&key, &old).unwrap();
    // The second PUT dies on the wire after half the body. The caller
    // must get an error, never a fake ack.
    let torn = through_proxy.put(&key, &stored);
    assert!(matches!(torn, Err(CasError::Io { .. })), "{torn:?}");
    // And the endpoint must still hold every byte of the old object.
    let (data, _) = direct.get(&key).unwrap().unwrap();
    assert_eq!(data, old);
    direct.delete(&key).unwrap();
}
