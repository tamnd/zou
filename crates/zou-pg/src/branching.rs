//! What has to be true of a branch before anybody is told it exists.
//!
//! [`zou_store::branch`](zou_store::branch) writes a child manifest and
//! nothing else, which is the whole point: no data is copied and the
//! call is two round trips. What it cannot know is whether the objects
//! that manifest names are enough to serve a database from. A child
//! reads inherited pages out of the captures it names and has no
//! fallback for the ones it cannot, so a source that has not folded a
//! full capture down yet yields a child that looks like a database in
//! every listing and fails on the first page read.
//!
//! So the two halves live here rather than in one caller: the check
//! that takes the child back off the store when it would not serve, and
//! the removal itself, which `zou branch delete` needs for a child that
//! did serve and is no longer wanted.

use zou_store::layout::TenantLayout;
use zou_store::{CasStore, Manifest};

/// Everything under a ref's prefix, and the count of what went.
///
/// The manifest goes first. Everything else under the prefix is
/// unreachable once it is gone, so a removal interrupted halfway leaves
/// objects nobody can read rather than a tenant whose manifest points
/// at objects that are no longer there.
pub fn discard(store: &dyn CasStore, tenant_ref: &str) -> Result<usize, String> {
    let layout = TenantLayout::new(tenant_ref);
    store
        .delete(&layout.manifest())
        .map_err(|e| format!("store: {e}"))?;
    let prefix = format!("{}/", layout.prefix());
    let keys = store.list(&prefix).map_err(|e| format!("store: {e}"))?;
    let mut deleted = 1;
    for key in &keys {
        store.delete(key).map_err(|e| format!("store: {e}"))?;
        deleted += 1;
    }
    Ok(deleted)
}

/// Refuse a child that would not serve, and leave nothing of it behind.
///
/// Asking costs one store round trip per inherited checkpoint, which is
/// the cheapest this answer is ever going to be, and it is asked while
/// somebody is still looking at the terminal rather than an hour later
/// on the first page read.
pub fn refuse_unservable(
    store: &dyn CasStore,
    src: &str,
    dst: &str,
    manifest: &Manifest,
) -> Result<(), String> {
    let Some(why) = crate::reader::why_unservable(store, &TenantLayout::new(dst), manifest)? else {
        return Ok(());
    };
    discard(store, dst)?;
    Err(format!(
        "{src} cannot be branched yet, {why}. A fold packs one down after a few checkpoints of \
         writes, so keep the source running and try again. Nothing of {dst} was left on the store"
    ))
}
