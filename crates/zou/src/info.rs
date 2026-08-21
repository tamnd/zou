//! `zou info <target> [ref]`: print manifest, checkpoint, and WAL stats
//! for one tenant, `local` by default.
//!
//! Everything comes from the tenant manifest, the log's shard manifest,
//! and one listing, so the command is safe against a live store: no
//! lease is taken and nothing is written. The wal stream end is the
//! tenant's last durable byte in its log, read the same way an
//! attaching node would read it.

use std::sync::Arc;

use zou_log::{ShardManifest, WalMedia, stream_end};
use zou_store::layout::TenantLayout;
use zou_store::manifest::CheckpointKind;
use zou_store::{CasStore, Manifest, PrefixStore, open_store, tenant_id};

pub const USAGE: &str = "usage: zou info <target> [ref]";

/// The single shard of a tenant's log in this release, the same
/// constant the pusher writes with.
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
        .ok_or_else(|| {
            format!(
                "{target} has no manifest for tenant {tenant}, `zou tenant {target} list` shows what is there"
            )
        })?;
    let manifest = Manifest::from_json(&data).map_err(|e| format!("manifest: {e}"))?;

    // The shard count is here because it is the authoritative one.
    // Every walk of the tree iterates it rather than trusting a
    // listing, so a person reconciling what they see under a prefix
    // against what the tenant actually has needs the number itself.
    say!(
        "ref {}, format {}, epoch {}, shards {}, pg {} timeline {}",
        manifest.tenant_ref,
        manifest.format,
        manifest.epoch,
        manifest.shards,
        manifest.pg.version,
        manifest.pg.timeline
    );
    match &manifest.lease {
        Some(l) => say!(
            "lease held by {} until unix {}, fence {}",
            l.holder,
            l.expires_unix,
            l.fence
        ),
        None => say!("lease free"),
    }
    if let Some(of) = &manifest.branch_of {
        say!("branch of {} at {:#X}", of.tenant_ref, of.at_lsn.0);
    }
    if let Some(unix) = manifest.published_unix {
        say!("last published unix {unix}");
    }

    say!("checkpoints: {}", manifest.checkpoints.len());
    for c in &manifest.checkpoints {
        let kind = match c.kind {
            CheckpointKind::Full => "full",
            CheckpointKind::Delta => "delta",
        };
        let owner = match &c.owner {
            Some(owner) => format!(", owned by {owner}"),
            None => String::new(),
        };
        say!("  {} {kind} at {:#X}{owner}", c.id, c.lsn.0);
    }

    if let Some(f) = manifest.folded_upto {
        say!("folded upto {:#X}", f.0);
    }
    let logs: Arc<dyn CasStore> =
        Arc::new(PrefixStore::over(Arc::clone(&store), &layout.log_prefix()));
    let media = WalMedia::single(Arc::clone(&logs));
    let end = stream_end(&media, WAL_SHARD, tenant_id(tenant)).map_err(|e| format!("wal: {e}"))?;
    match end {
        Some(lsn) => say!("wal stream end {:#X}", lsn.0),
        None => say!("wal stream end: nothing appended"),
    }
    match ShardManifest::load(&*logs, WAL_SHARD).map_err(|e| format!("wal: {e}"))? {
        Some((sm, _)) => say!(
            "log shard {}: chain epoch {}, head {}, consolidated upto {}, gc round {}",
            sm.shard,
            sm.chain_epoch,
            sm.head,
            sm.consolidated_upto,
            sm.gc_round
        ),
        None => say!("log: no shard manifest yet"),
    }

    let snapshots = store
        .list(&layout.manifests_dir())
        .map_err(|e| format!("store: {e}"))?;
    say!("history: {} manifest snapshots", snapshots.len());
    Ok(())
}
