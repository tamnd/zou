//! `zou branch <target> <create|list|delete>`: copy on write branches of
//! a tenant inside a store.
//!
//! `create <src> <dst>` with no flag branches at the source's newest
//! checkpoint. An `--at` value shaped like a Postgres LSN, `X/Y` or `0x`
//! hex, must name a checkpoint lsn exactly, branch points are checkpoint
//! lsns in this release. Plain digits are a unix second and materialize
//! the newest manifest history snapshot at or before it, which is the
//! point in time restore, and `--from-time <unix>` is that same thing
//! spelled out for people who would rather say what they mean. Either
//! way the child only references the parent's objects, nothing is
//! copied, so the call finishes in the time of two manifest round trips.
//! What the child would read is checked before the call returns, and a
//! source whose captures do not cover the whole chain is refused here
//! instead of an hour later on the first page read.
//!
//! `list` reads every manifest on the store and prints the ones that
//! name a parent, optionally only the children of one ref. `delete`
//! removes a branch and everything under its prefix, and it is careful
//! in three ways: it refuses a tenant that is not a branch, because a
//! root database is not something a typo should be able to remove; it
//! refuses one that has children of its own, because they read through
//! to objects it owns; and it refuses one whose lease is still live,
//! because something is attached to it right now. The registry entry
//! goes with it if there is one, and `zou tenant delete` stays the
//! gentler command that only forgets the ref.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use zou_store::layout::TenantLayout;
use zou_store::registry;
use zou_store::{CasStore, Lsn, Manifest, branch, materialize_at, open_store};

pub const USAGE: &str = "usage: zou branch <target> <create <src> <dst> [--at <ts|lsn> | --from-time <unix>] | list [parent] | delete <ref>>";

#[derive(Debug)]
enum At {
    Head,
    Lsn(u64),
    Ts(u64),
}

fn parse_at(value: &str) -> Result<At, String> {
    if let Some((hi, lo)) = value.split_once('/') {
        let hi = u64::from_str_radix(hi, 16);
        let lo = u64::from_str_radix(lo, 16);
        return match (hi, lo) {
            (Ok(hi), Ok(lo)) => Ok(At::Lsn((hi << 32) | lo)),
            _ => Err(format!("bad lsn {value:?}")),
        };
    }
    if let Some(hex) = value.strip_prefix("0x") {
        return u64::from_str_radix(hex, 16)
            .map(At::Lsn)
            .map_err(|_| format!("bad lsn {value:?}"));
    }
    value
        .parse()
        .map(At::Ts)
        .map_err(|_| format!("bad timestamp {value:?}"))
}

/// `--from-time` only ever means a unix second. An lsn shaped value
/// here is somebody reaching for `--at`, and saying so is kinder than
/// branching a database somewhere they did not ask for.
fn parse_from_time(value: &str) -> Result<At, String> {
    match parse_at(value)? {
        At::Ts(ts) => Ok(At::Ts(ts)),
        _ => Err(format!(
            "--from-time takes a unix second, {value:?} looks like an lsn, use --at for that"
        )),
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn run(argv: &[String]) -> Result<(), String> {
    let [target, rest @ ..] = argv else {
        return Err(USAGE.into());
    };
    let store: Arc<dyn CasStore> = Arc::from(open_store(target)?);
    match rest {
        [verb, src, dst] if verb == "create" => create(store.as_ref(), src, dst, At::Head),
        [verb, src, dst, flag, value] if verb == "create" && flag == "--at" => {
            create(store.as_ref(), src, dst, parse_at(value)?)
        }
        [verb, src, dst, flag, value] if verb == "create" && flag == "--from-time" => {
            create(store.as_ref(), src, dst, parse_from_time(value)?)
        }
        [verb] if verb == "list" => list(store.as_ref(), None),
        [verb, parent] if verb == "list" => list(store.as_ref(), Some(parent)),
        [verb, tenant_ref] if verb == "delete" => delete(store.as_ref(), tenant_ref),
        _ => Err(USAGE.into()),
    }
}

fn create(store: &dyn CasStore, src: &str, dst: &str, at: At) -> Result<(), String> {
    let now = now();
    let manifest = match at {
        At::Head => branch(store, src, dst, None, now),
        At::Lsn(lsn) => branch(store, src, dst, Some(Lsn(lsn)), now),
        At::Ts(ts) => materialize_at(store, src, dst, ts, now),
    }
    .map_err(|e| e.to_string())?;
    // A child reads inherited pages out of the captures it names and
    // has no fallback for the ones it cannot, so a source whose
    // captures do not cover the whole chain yields a database that
    // looks fine here and fails on the first page read an hour later.
    // Asking now costs one store round trip per inherited checkpoint,
    // and a child that would not serve is taken back off the store
    // rather than left as something only shaped like a database.
    match zou_pg::reader::why_unservable(store, &TenantLayout::new(dst), &manifest)? {
        None => {}
        Some(why) => {
            discard(store, dst)?;
            return Err(format!(
                "{src} cannot be branched yet, {why}. A fold packs one down after a few \
                 checkpoints of writes, so keep the source running and try again. Nothing of \
                 {dst} was left on the store"
            ));
        }
    }
    let of = manifest
        .branch_of
        .as_ref()
        .ok_or("a child names its parent")?;
    println!(
        "branched {src} into {dst} at {:#X}, {} checkpoints inherited",
        of.at_lsn.0,
        manifest.checkpoints.len()
    );
    Ok(())
}

/// Every ref with a manifest on the store, in listing order. One list
/// call answers this for a store of any size, and a prefix that holds
/// no manifest is a ref that was never bootstrapped.
fn refs(store: &dyn CasStore) -> Result<Vec<String>, String> {
    let keys = store.list("tenants/").map_err(|e| format!("store: {e}"))?;
    let mut found = Vec::new();
    for key in &keys {
        if let Some(rest) = key.strip_prefix("tenants/")
            && let Some((r, tail)) = rest.split_once('/')
            && tail == "MANIFEST"
        {
            found.push(r.to_string());
        }
    }
    Ok(found)
}

fn read_manifest(store: &dyn CasStore, tenant_ref: &str) -> Result<Option<Manifest>, String> {
    let key = TenantLayout::new(tenant_ref).manifest();
    let Some((data, _)) = store.get(&key).map_err(|e| format!("store: {e}"))? else {
        return Ok(None);
    };
    Manifest::from_json(&data)
        .map(Some)
        .map_err(|e| format!("manifest for {tenant_ref}: {e}"))
}

fn list(store: &dyn CasStore, parent: Option<&str>) -> Result<(), String> {
    let mut lines = 0;
    for tenant_ref in refs(store)? {
        let Some(manifest) = read_manifest(store, &tenant_ref)? else {
            continue;
        };
        let Some(of) = &manifest.branch_of else {
            continue;
        };
        if parent.is_some_and(|p| p != of.tenant_ref) {
            continue;
        }
        println!(
            "{tenant_ref}\tfrom {} at {:#X}, {} checkpoints",
            of.tenant_ref,
            of.at_lsn.0,
            manifest.checkpoints.len()
        );
        lines += 1;
    }
    match (lines, parent) {
        (0, Some(p)) => println!("no branches of {p}"),
        (0, None) => println!("no branches on this store"),
        (n, _) => println!("{n} branches"),
    }
    Ok(())
}

/// Everything under a ref's prefix, and the count of what went. The
/// manifest goes first. Everything else under the prefix is
/// unreachable once it is gone, so a removal interrupted halfway
/// leaves objects nobody can read rather than a tenant whose manifest
/// points at objects that are no longer there.
fn discard(store: &dyn CasStore, tenant_ref: &str) -> Result<usize, String> {
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

fn delete(store: &dyn CasStore, tenant_ref: &str) -> Result<(), String> {
    let manifest = read_manifest(store, tenant_ref)?
        .ok_or_else(|| format!("no database at {}", TenantLayout::new(tenant_ref).prefix()))?;
    if manifest.branch_of.is_none() {
        return Err(format!(
            "{tenant_ref} is not a branch, its objects are its own, remove {} by hand to delete it",
            TenantLayout::new(tenant_ref).prefix()
        ));
    }
    if let Some(lease) = &manifest.lease
        && lease.expires_unix > now()
    {
        return Err(format!(
            "{tenant_ref} is leased by {} until unix {}, detach it first",
            lease.holder, lease.expires_unix
        ));
    }
    let children: Vec<String> = refs(store)?
        .into_iter()
        .filter(|r| r != tenant_ref)
        .filter(|r| {
            read_manifest(store, r)
                .ok()
                .flatten()
                .and_then(|m| m.branch_of)
                .is_some_and(|of| of.tenant_ref == tenant_ref)
        })
        .collect();
    if !children.is_empty() {
        return Err(format!(
            "{tenant_ref} still has branches of its own: {}",
            children.join(", ")
        ));
    }

    let deleted = discard(store, tenant_ref)?;
    match registry::delete(store, tenant_ref) {
        Ok(()) => println!("unregistered {tenant_ref}"),
        // A branch nobody registered is the common case, the registry
        // is a router's index and branching does not touch it.
        Err(registry::RegistryError::Missing { .. }) => {}
        Err(e) => return Err(e.to_string()),
    }
    println!("deleted branch {tenant_ref}, {deleted} objects");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use zou_store::LocalFsStore;
    use zou_store::manifest::{CheckpointKind, CheckpointRef};

    /// A root database with one checkpoint, which is the least a
    /// branch can be taken from.
    fn root(store: &dyn CasStore, tenant_ref: &str) {
        let mut m = Manifest::new(tenant_ref, 18);
        m.checkpoints.push(CheckpointRef {
            id: "genesis".into(),
            lsn: Lsn(0x100),
            kind: CheckpointKind::Full,
            owner: None,
        });
        store
            .put(&TenantLayout::new(tenant_ref).manifest(), &m.to_json())
            .unwrap();
    }

    /// The same, with the full capture bearing a page run index, which
    /// is what a database that has run long enough to fold one looks
    /// like and the only kind a branch can be served from.
    fn folded_root(store: &dyn CasStore, tenant_ref: &str) {
        let layout = TenantLayout::new(tenant_ref);
        let mut m = Manifest::new(tenant_ref, 18);
        m.checkpoints.push(CheckpointRef {
            id: "f1".into(),
            lsn: Lsn(0x100),
            kind: CheckpointKind::Full,
            owner: None,
        });
        store
            .put(
                &layout.checkpoint_page_index("f1"),
                b"runs 1024\np 1663 5 16384 0 0\n",
            )
            .unwrap();
        store.put(&layout.manifest(), &m.to_json()).unwrap();
    }

    /// A pull request database that answers nothing is worse than one
    /// that was never made, because the job that made it went green.
    #[test]
    fn create_refuses_a_source_the_child_could_not_read() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        root(&store, "prod");

        let err = create(&store, "prod", "pr-1", At::Head).unwrap_err();
        assert!(err.contains("cannot be branched yet"), "{err}");
        assert!(err.contains("run bearing full capture"), "{err}");
        // And nothing of the child is left on the store, so the next
        // run after the source has folded one is not a name collision.
        assert!(read_manifest(&store, "pr-1").unwrap().is_none());
        assert!(store.list("tenants/pr-1/").unwrap().is_empty());

        folded_root(&store, "ready");
        create(&store, "ready", "pr-1", At::Head).unwrap();
        assert!(read_manifest(&store, "pr-1").unwrap().is_some());
    }

    #[test]
    fn at_values_disambiguate_by_shape() {
        assert!(matches!(parse_at("1/2A"), Ok(At::Lsn(0x1_0000_002A))));
        assert!(matches!(parse_at("0x1F4C118"), Ok(At::Lsn(0x1F4C118))));
        assert!(matches!(parse_at("1767100000"), Ok(At::Ts(1767100000))));
        assert!(parse_at("hot").is_err());
        assert!(parse_at("1/hot").is_err());
        assert!(parse_at("0xzz").is_err());
    }

    #[test]
    fn from_time_is_a_timestamp_and_says_so_when_it_is_not() {
        assert!(matches!(parse_from_time("1767100000"), Ok(At::Ts(_))));
        let err = parse_from_time("0/1F4C118").unwrap_err();
        assert!(err.contains("--at"), "{err}");
    }

    /// A root database is not something one mistyped word should be
    /// able to remove, and a branch with a branch of its own owns
    /// objects that one reads through.
    #[test]
    fn delete_refuses_a_root_and_refuses_a_parent() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        root(&store, "prod");
        branch(&store, "prod", "pr-142", None, 1000).unwrap();
        branch(&store, "pr-142", "pr-142-fix", None, 1000).unwrap();

        let err = delete(&store, "prod").unwrap_err();
        assert!(err.contains("is not a branch"), "{err}");
        let err = delete(&store, "pr-142").unwrap_err();
        assert!(err.contains("pr-142-fix"), "{err}");

        delete(&store, "pr-142-fix").unwrap();
        // Now that the child is gone the parent branch can go too, and
        // the root is still where it was.
        delete(&store, "pr-142").unwrap();
        assert!(read_manifest(&store, "pr-142").unwrap().is_none());
        assert!(read_manifest(&store, "prod").unwrap().is_some());
    }

    #[test]
    fn delete_takes_the_whole_prefix_and_the_registry_entry() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        root(&store, "prod");
        branch(&store, "prod", "pr-9", None, 1000).unwrap();
        let layout = TenantLayout::new("pr-9");
        store
            .put(&layout.chk_index("aaa"), b"f base/one 100\n")
            .unwrap();
        store
            .put_if_absent("tenants/pr-9/log/cellwal/0000/0000000000000001", b"wal")
            .unwrap();
        registry::create(&store, &registry::Tenant::new("pr-9", "secret", 1000)).unwrap();

        delete(&store, "pr-9").unwrap();
        assert!(store.list("tenants/pr-9/").unwrap().is_empty());
        assert!(registry::get(&store, "pr-9").unwrap().is_none());
        // The parent kept everything, a branch owns only what it wrote.
        assert!(read_manifest(&store, "prod").unwrap().is_some());
    }

    #[test]
    fn list_names_the_parent_and_can_be_asked_about_one() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path());
        root(&store, "prod");
        branch(&store, "prod", "pr-1", None, 1000).unwrap();
        branch(&store, "pr-1", "pr-1-fix", None, 1000).unwrap();
        // Both spellings run against a store that has branches and one
        // that has none, the printing is what the test is watching.
        list(&store, None).unwrap();
        list(&store, Some("prod")).unwrap();
        list(&store, Some("nobody")).unwrap();
        // Key order, not ref order: "pr-1-fix/" sorts before "pr-1/"
        // because a dash is below a slash.
        assert_eq!(refs(&store).unwrap(), vec!["pr-1-fix", "pr-1", "prod"]);
    }
}
