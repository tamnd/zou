//! A tour of the object layer on a local directory: genesis manifest,
//! writer lease, group committed WAL, sealed segments, manifest tail.
//!
//! Run it with `make demo` or `cargo run -p zou-store --example demo`.
//! Everything lands in a temp directory that is printed and kept, so you
//! can poke at the objects afterwards.

use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use zou_store::layout::TenantLayout;
use zou_store::lease;
use zou_store::{
    CasStore, GroupCommit, GroupCommitConfig, LocalFsStore, Lsn, Manifest, TailConfig,
};

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
    // its WAL directory is simply never referenced again.
    let held = lease::acquire(&*store, &layout, "demo-writer", 15, now()).expect("lease acquire");
    println!(
        "lease acquired  holder=demo-writer epoch={} fence={}",
        held.epoch, held.fence
    );
    let held = Arc::new(Mutex::new(held));

    // Group commit: appends return tickets, tickets resolve when the
    // frame batch is durable on the store. No ack before durability.
    let commit = GroupCommit::with_lease(
        Arc::clone(&store),
        layout.clone(),
        Arc::clone(&held),
        Lsn(1),
        GroupCommitConfig::default(),
        TailConfig::default(),
    );
    let tickets: Vec<_> = (0..100)
        .map(|i| {
            let record = format!("insert into demo values ({i})");
            commit.append(record.as_bytes()).expect("append")
        })
        .collect();
    let mut durable = Lsn(0);
    for ticket in tickets {
        durable = ticket.wait().expect("durable flush");
    }
    println!("committed       100 records, durable through lsn {durable:?}");

    // Close seals the open segment and publishes wal_tail, so a clean
    // shutdown leaves an exact manifest behind.
    commit.close().expect("close pipeline");

    let (data, _) = store
        .get(&layout.manifest())
        .expect("read")
        .expect("manifest");
    let manifest = Manifest::from_json(&data).expect("parse");
    let tail = manifest.wal_tail.as_ref().expect("tail published");
    println!(
        "wal tail        epoch_dir={} from_lsn={:?} segments={}",
        tail.epoch_dir,
        tail.from_lsn,
        tail.segments.len()
    );

    // Graceful detach clears the lease so the next writer starts at once
    // instead of waiting out the TTL.
    let held = Arc::try_unwrap(held)
        .expect("sole owner")
        .into_inner()
        .unwrap();
    lease::release(&*store, &layout, held).expect("release");
    println!("lease released  next writer can attach immediately");

    println!("objects:");
    for key in store.list(layout.prefix()).expect("list") {
        println!("  {key}");
    }
}
