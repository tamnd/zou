//! `zou info <target> [ref]`: print manifest, checkpoint, and WAL stats
//! for one tenant, `local` by default.
//!
//! Everything comes from the tenant manifest, the shared log's shard
//! manifest, and one listing, so the command is safe against a live
//! store: no lease is taken and nothing is written. The wal stream end
//! is the tenant's last durable byte in the shared log, read the same
//! way an attaching node would read it.

use std::sync::Arc;

use zou_log::{ShardManifest, WalMedia, stream_end};
use zou_store::layout::TenantLayout;
use zou_store::manifest::CheckpointKind;
use zou_store::{CasStore, Manifest, open_store, tenant_id};

pub const USAGE: &str = "usage: zou info <target> [ref]";

/// The single shared log shard in this release, the same constant the
/// pusher writes with.
const WAL_SHARD: u32 = 0;

pub fn run(argv: &[String]) -> Result<(), String> {
    let (target, tenant) = match argv {
        [target] => (target.as_str(), "local"),
        [target, tenant] => (target.as_str(), tenant.as_str()),
        _ => return Err(USAGE.into()),
    };
    let store: Arc<dyn CasStore> = Arc::from(open_store(target)?);
    let layout = TenantLayout::new(tenant);
    let (data, _) = store
        .get(&layout.manifest())
        .map_err(|e| format!("store: {e}"))?
        .ok_or_else(|| format!("{target} has no manifest for tenant {tenant}"))?;
    let manifest = Manifest::from_json(&data).map_err(|e| format!("manifest: {e}"))?;

    println!(
        "ref {}, format {}, epoch {}, pg {} timeline {}",
        manifest.tenant_ref,
        manifest.format,
        manifest.epoch,
        manifest.pg.version,
        manifest.pg.timeline
    );
    match &manifest.lease {
        Some(l) => println!(
            "lease held by {} until unix {}, fence {}",
            l.holder, l.expires_unix, l.fence
        ),
        None => println!("lease free"),
    }
    if let Some(of) = &manifest.branch_of {
        println!("branch of {} at {:#X}", of.tenant_ref, of.at_lsn.0);
    }
    if let Some(unix) = manifest.published_unix {
        println!("last published unix {unix}");
    }

    println!("checkpoints: {}", manifest.checkpoints.len());
    for c in &manifest.checkpoints {
        let kind = match c.kind {
            CheckpointKind::Full => "full",
            CheckpointKind::Delta => "delta",
        };
        let owner = match &c.owner {
            Some(owner) => format!(", owned by {owner}"),
            None => String::new(),
        };
        println!("  {} {kind} at {:#X}{owner}", c.id, c.lsn.0);
    }

    if let Some(f) = manifest.folded_upto {
        println!("folded upto {:#X}", f.0);
    }
    let media = WalMedia::single(Arc::clone(&store));
    let end = stream_end(&media, WAL_SHARD, tenant_id(tenant)).map_err(|e| format!("wal: {e}"))?;
    match end {
        Some(lsn) => println!("wal stream end {:#X}", lsn.0),
        None => println!("wal stream end: nothing appended"),
    }
    match ShardManifest::load(&*store, WAL_SHARD).map_err(|e| format!("wal: {e}"))? {
        Some((sm, _)) => println!(
            "shared log shard {}: chain epoch {}, head {}, consolidated upto {}, gc round {}",
            sm.shard, sm.chain_epoch, sm.head, sm.consolidated_upto, sm.gc_round
        ),
        None => println!("shared log: no shard manifest yet"),
    }

    let snapshots = store
        .list(&layout.manifests_dir())
        .map_err(|e| format!("store: {e}"))?;
    println!("history: {} manifest snapshots", snapshots.len());
    Ok(())
}
