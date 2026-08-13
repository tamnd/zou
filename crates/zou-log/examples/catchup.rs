//! Ask a store on disk what catch up would deliver, for chasing a page
//! service that has stopped applying. Not part of the product, just the
//! smallest program that answers "is the stream there and does catch up
//! hand it over".
//!
//! usage: catchup <store-log-dir> <shard> <tenant> <applied-hex>

use std::sync::Arc;

use zou_log::{
    CatchUpCursor, ConsolidateError, RoundIndex, ShardManifest, TeeFilter, WalMedia,
    catch_up_resuming, stream_end,
};
use zou_store::{CasStore, LocalFsStore, Lsn};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        eprintln!("usage: catchup <store-log-dir> <shard> <tenant> <applied-hex>");
        std::process::exit(2);
    }
    let dir = &args[1];
    let shard: u32 = args[2].parse().expect("shard");
    let tenant: u128 = args[3].parse().expect("tenant");
    let applied = u64::from_str_radix(args[4].trim_start_matches("0x"), 16).expect("applied");

    let store: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(std::path::Path::new(dir)));
    let media = WalMedia::single(Arc::clone(&store));

    let manifest = ShardManifest::load(store.as_ref(), shard)
        .expect("manifest load")
        .map(|(m, _)| m);
    match &manifest {
        Some(m) => println!(
            "manifest: head {} consolidated_upto {} rounds {:?}",
            m.head, m.consolidated_upto, m.rounds
        ),
        None => println!("manifest: none"),
    }
    if let Some(rounds) = manifest.as_ref().and_then(|m| m.rounds) {
        for round in [rounds.first, rounds.last] {
            let index = RoundIndex::load(store.as_ref(), shard, round)
                .expect("round load")
                .expect("round present");
            println!(
                "round {round}: seq {}..{} tenants {:?}",
                index.first_seq,
                index.last_seq,
                index
                    .tenants
                    .iter()
                    .map(|t| (t.tenant, t.watermark, t.frames))
                    .collect::<Vec<_>>()
            );
        }
    }

    match stream_end(&media, shard, tenant) {
        Ok(Some(end)) => println!("stream_end: {:#x}", end.0),
        Ok(None) => println!("stream_end: none"),
        Err(e) => println!("stream_end: error {e}"),
    }

    let filter = TeeFilter::Tenant(tenant);
    let mut cursor = CatchUpCursor::default();
    let mut frames = 0usize;
    let mut first = None;
    let mut last = None;
    let out = catch_up_resuming::<ConsolidateError, _, _>(
        &media,
        shard,
        &filter,
        Lsn(applied),
        &mut cursor,
        |frame| {
            frames += 1;
            if first.is_none() {
                first = Some(frame.start_lsn.0);
            }
            last = Some(frame.end_lsn.0);
            Ok(frames < 20000)
        },
        || true,
    );
    println!(
        "catch_up from {applied:#x}: frames {frames} first {first:x?} last {last:x?} out {out:x?} cursor {cursor:?}"
    );
}
