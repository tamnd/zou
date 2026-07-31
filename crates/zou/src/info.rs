//! `zou info <target> [ref]`: print manifest, checkpoint, and WAL stats
//! for one tenant, `local` by default.
//!
//! Everything comes from the manifest plus two listings, so the command
//! is safe against a live store: no lease is taken and nothing is
//! written. The WAL tail is reconciled against the wal/ listing the
//! same way an attaching node would see it, so segments sealed after
//! the last manifest publish are counted too.

use std::sync::Arc;

use zou_store::commit::reconcile_tail;
use zou_store::layout::TenantLayout;
use zou_store::manifest::CheckpointKind;
use zou_store::{CasStore, Manifest, open_store};

pub const USAGE: &str = "usage: zou info <target> [ref]";

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

    for pt in &manifest.parent_tail {
        println!(
            "parent tail from {}: {} segments from {:#X}",
            pt.tenant_ref,
            pt.segments.len(),
            pt.from_lsn.0
        );
    }
    let tail = reconcile_tail(&*store, &layout, &manifest).map_err(|e| format!("store: {e}"))?;
    match tail {
        Some(t) => println!(
            "wal tail: {} segments from {:#X}, epoch dir {}",
            t.segments.len(),
            t.from_lsn.0,
            t.epoch_dir
        ),
        None => println!("wal tail: empty"),
    }

    let snapshots = store
        .list(&layout.manifests_dir())
        .map_err(|e| format!("store: {e}"))?;
    println!("history: {} manifest snapshots", snapshots.len());
    Ok(())
}
