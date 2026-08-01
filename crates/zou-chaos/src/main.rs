//! Standalone chaos proxy for manual runs: point ZOU_S3_ENDPOINT at it
//! and watch retries in the logs. Configuration through the environment:
//!
//!   ZOU_CHAOS_UPSTREAM            host:port of the real endpoint, required
//!   ZOU_CHAOS_LISTEN              listen address, default 127.0.0.1:9500
//!   ZOU_CHAOS_ERROR_EVERY         503 every Nth request, 0 off
//!   ZOU_CHAOS_DELAY_EVERY         delay every Nth request, 0 off
//!   ZOU_CHAOS_DELAY_MS            delay length in milliseconds
//!   ZOU_CHAOS_TRUNCATE_PUT_EVERY  cut every Nth PUT in half, 0 off

use std::time::Duration;

use zou_chaos::{ChaosConfig, spawn};

fn main() {
    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    let num = |name: &str| var(name).and_then(|v| v.parse().ok()).unwrap_or(0);
    let Some(upstream) = var("ZOU_CHAOS_UPSTREAM") else {
        eprintln!("ZOU_CHAOS_UPSTREAM must name the real endpoint as host:port");
        std::process::exit(2);
    };
    let cfg = ChaosConfig {
        upstream,
        error_every: num("ZOU_CHAOS_ERROR_EVERY"),
        delay_every: num("ZOU_CHAOS_DELAY_EVERY"),
        delay_ms: num("ZOU_CHAOS_DELAY_MS"),
        truncate_put_every: num("ZOU_CHAOS_TRUNCATE_PUT_EVERY"),
    };
    let listen = var("ZOU_CHAOS_LISTEN").unwrap_or_else(|| "127.0.0.1:9500".into());
    match spawn(&listen, cfg) {
        Ok(proxy) => {
            println!("zou-chaos listening on {}", proxy.addr());
            loop {
                std::thread::sleep(Duration::from_secs(3600));
            }
        }
        Err(e) => {
            eprintln!("cannot listen on {listen}: {e}");
            std::process::exit(1);
        }
    }
}
