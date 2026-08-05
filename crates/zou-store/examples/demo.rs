//! A tour of the object layer on a local directory: genesis manifest,
//! writer lease, manifest publish, graceful release.
//!
//! Run it with `make demo` or `cargo run -p zou-store --example demo`.
//! Everything lands in a temp directory that is printed and kept, so you
//! can poke at the objects afterwards. WAL lives in the shared log now,
//! see the zou-log crate for the append path.

use std::sync::Arc;
use std::time::SystemTime;

use zou_store::layout::TenantLayout;
use zou_store::lease;
use zou_store::{CasStore, LocalFsStore, Manifest};

fn main() {
    let dir = std::env::temp_dir().join(format!("zou-demo-{}", std::process::id()));
    let store: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(&dir));
    let layout = TenantLayout::new("demo");
    let now = || {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    };

    println!("store root      {}", dir.display());

    // Genesis: the manifest is the root of truth and the only mutable
    // object. Creating it with expected=None means a tenant can only be
    // born once.
    let manifest = Manifest::new("demo", 18);
    store
        .put_if_match(&layout.manifest(), &manifest.to_json(), None)
        .expect("genesis manifest create");
    println!("genesis         {}", layout.manifest());

    // Become the writer. The epoch bump fences out any earlier holder:
    // frames it appends under the stale epoch are rejected on read.
    let held = lease::acquire(&*store, &layout, "demo-writer", 15, now()).expect("lease acquire");
    println!(
        "lease acquired  holder=demo-writer epoch={} fence={}",
        held.epoch, held.fence
    );

    // Graceful detach clears the lease so the next writer starts at once
    // instead of waiting out the TTL.
    lease::release(&*store, &layout, held).expect("release");
    println!("lease released  next writer can attach immediately");

    println!("objects:");
    for key in store.list(layout.prefix()).expect("list") {
        println!("  {key}");
    }
}
