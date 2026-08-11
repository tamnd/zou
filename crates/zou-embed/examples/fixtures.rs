//! How long a fixture database takes, said as a distribution.
//!
//! An average hides the one that took a second, and the one that took a
//! second is the one a test suite feels, so this reports the spread and
//! the slowest.
//!
//!   ZOU_PG_BIN=$PWD/build/pg/bin cargo run --release -p zou-embed \
//!       --example fixtures -- 20
//!
//! The first run on a machine builds the template and reports that
//! separately, because it is a different question with a different
//! answer. Pass `--phases` to break a single create down instead.

#![cfg(unix)]

use std::path::PathBuf;
use std::time::Instant;

use zou_embed::{Options, Zou};

fn main() {
    // `RUST_LOG=zou_embed=debug` breaks a create into the branch, the
    // restore, the postmaster and the front door.
    env_logger::init();
    let mut rounds = 20usize;
    let mut phases = false;
    for arg in std::env::args().skip(1) {
        if arg == "--phases" {
            phases = true;
        } else if let Ok(n) = arg.parse() {
            rounds = n;
        }
    }
    let pg_bin = match std::env::var("ZOU_PG_BIN") {
        Ok(v) if !v.is_empty() => PathBuf::from(v),
        _ => PathBuf::from("build/pg/bin"),
    };
    let options = || Options {
        pg_bin: pg_bin.clone(),
        ..Options::fixture()
    };

    // The template, if this machine has not built one, plus one create.
    // The two are together in this number and apart in every later one.
    let cold = Instant::now();
    let first = Zou::open(options()).expect("the first fixture");
    let cold = cold.elapsed();
    println!("template and the first create: {}", ms(cold.as_secs_f64()));
    println!("template store: {}", first.target());
    first.close().expect("close");

    if phases {
        return;
    }

    let mut took = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let at = Instant::now();
        let zou = Zou::open(options()).expect("a fixture");
        took.push(at.elapsed().as_secs_f64() * 1000.0);
        zou.close().expect("close");
    }
    took.sort_by(f64::total_cmp);

    println!("{rounds} creates against a warm template, milliseconds:");
    for (name, at) in [("p50", 0.50), ("p90", 0.90), ("p99", 0.99)] {
        let i = ((took.len() as f64 * at).ceil() as usize).saturating_sub(1);
        println!("  {name}  {:.1}", took[i]);
    }
    println!("  min  {:.1}", took[0]);
    println!("  max  {:.1}", took[took.len() - 1]);
}

fn ms(seconds: f64) -> String {
    format!("{:.1} ms", seconds * 1000.0)
}
