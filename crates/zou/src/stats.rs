//! `zou stats <counter-file>`: dump the store op counters one run
//! accumulated, as json on stdout.
//!
//! The counter file is what `ZOU_STORE_STATS` pointed at, `zou dev`
//! keeps one at `<runtime>/store-stats` and logs the path on boot. The
//! dump is a cold read of the file, so it is safe to run while the
//! store is live and the harness scrapes it after every benchmark run.

use std::path::Path;

use zou_store::stats::Snapshot;

pub const USAGE: &str = "usage: zou stats <counter-file>";

pub fn run(argv: &[String]) -> Result<(), String> {
    let [path] = argv else {
        return Err(USAGE.into());
    };
    let snapshot = Snapshot::read(Path::new(path))?;
    println!("{}", snapshot.to_json());
    Ok(())
}
