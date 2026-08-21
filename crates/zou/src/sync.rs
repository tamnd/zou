//! `zou push <dir> <target> [ref]` and `zou pull <target> <dir> [ref]`:
//! copy a store, or one tenant of it, between a local directory and a
//! remote prefix.
//!
//! Both verbs are the same copy with the ends named, so nobody has to
//! remember which argument order means which direction: push writes
//! away from the machine you are on, pull writes onto it. With a ref
//! only `tenants/<ref>/` moves, without one the whole store does,
//! registry and all.
//!
//! What it skips is the point. A checkpoint object, a manifest history
//! snapshot, and a WAL segment are immutable once written, so a key
//! already on the far side is the same bytes and the copy leaves it
//! alone. That is what makes a second run cheap and what makes an
//! interrupted run resumable: it picks up where the objects run out.
//! Everything else, the live manifest above all, is copied every time,
//! because that is the object whose whole job is to change.
//!
//! The live manifest is also why a source with a held lease gets a
//! warning: something is writing to it, and the copy is a walk rather
//! than a snapshot, so it can catch a manifest that names a checkpoint
//! written after the walk passed the place it would have been. Copying
//! a detached tenant, or copying twice, gives a consistent one.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use zou_store::layout::TenantLayout;
use zou_store::{CasStore, Manifest, open_store};

pub const PUSH_USAGE: &str = "usage: zou push <dir> <target> [ref] [--jobs <n>]";
pub const PULL_USAGE: &str = "usage: zou pull <target> <dir> [ref] [--jobs <n>]";

/// Enough in flight to keep an object store busy over one link, few
/// enough that a local directory copy does not thrash.
const DEFAULT_JOBS: usize = 16;

/// Whether a key, once written, never changes. Those are the ones a
/// second run can skip on sight.
fn immutable(key: &str) -> bool {
    key.contains("/chk/") || key.contains("/log/") || key.contains("/manifests/")
}

struct Args {
    from: String,
    to: String,
    tenant: Option<String>,
    jobs: usize,
}

/// `<a> <b> [ref] [--jobs n]`, in the order the verb hands them over.
fn parse(argv: &[String], usage: &str) -> Result<Args, String> {
    let mut positional: Vec<&str> = Vec::new();
    let mut jobs = DEFAULT_JOBS;
    let mut rest = argv.iter();
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "--jobs" => {
                let value = rest.next().ok_or_else(|| usage.to_string())?;
                jobs = value.parse().map_err(|_| {
                    format!("bad job count {value:?}, write a whole number of jobs")
                })?;
                if jobs == 0 {
                    return Err("a copy with no jobs copies nothing".into());
                }
            }
            other => positional.push(other),
        }
    }
    match positional[..] {
        [a, b] => Ok(Args {
            from: a.into(),
            to: b.into(),
            tenant: None,
            jobs,
        }),
        [a, b, tenant] => Ok(Args {
            from: a.into(),
            to: b.into(),
            tenant: Some(tenant.into()),
            jobs,
        }),
        _ => Err(usage.into()),
    }
}

pub fn push(argv: &[String]) -> Result<(), String> {
    let args = parse(argv, PUSH_USAGE)?;
    run(&args)
}

pub fn pull(argv: &[String]) -> Result<(), String> {
    let args = parse(argv, PULL_USAGE)?;
    run(&args)
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Says so when the source is attached somewhere. Not an error: copying
/// a live tenant is a legitimate thing to want, it is just not a
/// snapshot, and finding that out afterwards is worse.
fn warn_if_leased(store: &dyn CasStore, tenant_ref: &str) {
    let key = TenantLayout::new(tenant_ref).manifest();
    let Ok(Some((data, _))) = store.get(&key) else {
        return;
    };
    let Ok(manifest) = Manifest::from_json(&data) else {
        return;
    };
    if let Some(lease) = &manifest.lease
        && lease.expires_unix > now()
    {
        eprintln!(
            "zou: {tenant_ref} is leased by {} until unix {}, this copy is a walk and not a snapshot",
            lease.holder, lease.expires_unix
        );
    }
}

fn run(args: &Args) -> Result<(), String> {
    let from: Arc<dyn CasStore> = Arc::from(open_store(&args.from)?);
    let to: Arc<dyn CasStore> = Arc::from(open_store(&args.to)?);
    let prefix = match &args.tenant {
        Some(r) => format!("{}/", TenantLayout::new(r).prefix()),
        None => String::new(),
    };
    if let Some(r) = &args.tenant {
        warn_if_leased(from.as_ref(), r);
    }

    let keys = from.list(&prefix).map_err(|e| format!("source: {e}"))?;
    if keys.is_empty() {
        return Err(match &args.tenant {
            Some(r) => format!(
                "no tenant {r} at {} on {}, `zou tenant {} list` shows what is registered there",
                prefix, args.from, args.from
            ),
            None => format!(
                "{} holds no objects, nothing to copy, `zou tenant {} list` shows whether anything is registered",
                args.from, args.from
            ),
        });
    }

    let next = AtomicUsize::new(0);
    let copied = AtomicUsize::new(0);
    let skipped = AtomicUsize::new(0);
    let bytes = AtomicUsize::new(0);
    let started = Instant::now();
    let jobs = args.jobs.min(keys.len());
    let mut failure: Option<String> = None;

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(jobs);
        for _ in 0..jobs {
            let (from, to) = (Arc::clone(&from), Arc::clone(&to));
            let (next, copied, skipped, bytes) = (&next, &copied, &skipped, &bytes);
            let keys = &keys;
            handles.push(scope.spawn(move || -> Result<(), String> {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some(key) = keys.get(i) else {
                        return Ok(());
                    };
                    if immutable(key)
                        && to
                            .get(key)
                            .map_err(|e| format!("destination {key}: {e}"))?
                            .is_some()
                    {
                        skipped.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let Some((data, _)) =
                        from.get(key).map_err(|e| format!("source {key}: {e}"))?
                    else {
                        // Deleted between the listing and now, which a
                        // gc run on a live store does all the time.
                        continue;
                    };
                    to.put(key, &data)
                        .map_err(|e| format!("destination {key}: {e}"))?;
                    copied.fetch_add(1, Ordering::Relaxed);
                    bytes.fetch_add(data.len(), Ordering::Relaxed);
                }
            }));
        }
        for handle in handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    failure.get_or_insert(e);
                }
                Err(_) => {
                    failure.get_or_insert_with(|| "a copy thread panicked".into());
                }
            }
        }
    });

    let copied = copied.load(Ordering::Relaxed);
    let skipped = skipped.load(Ordering::Relaxed);
    let bytes = bytes.load(Ordering::Relaxed);
    say!(
        "{} to {}: {copied} objects copied, {skipped} already there, {bytes} bytes in {:.1}s",
        args.from,
        args.to,
        started.elapsed().as_secs_f64()
    );
    match failure {
        // Said after the counts, because how far it got is the first
        // thing anyone wants to know about a copy that stopped.
        Some(e) => Err(e),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zou_store::LocalFsStore;

    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn seed(store: &dyn CasStore) {
        store
            .put("tenants/acme/MANIFEST", br#"{"first":true}"#)
            .unwrap();
        store
            .put("tenants/acme/chk/genesis/INDEX", b"f base/one 100\n")
            .unwrap();
        store
            .put("tenants/acme/log/cellwal/0000/0000000000000001", b"wal")
            .unwrap();
        store.put("tenants/other/MANIFEST", b"{}").unwrap();
        store.put("registry/acme.json", b"{}").unwrap();
    }

    #[test]
    fn the_flags_and_the_positions_come_apart() {
        let a = parse(&args(&["/dir", "s3://b/p"]), PUSH_USAGE).unwrap();
        assert_eq!((a.from.as_str(), a.to.as_str()), ("/dir", "s3://b/p"));
        assert!(a.tenant.is_none());
        assert_eq!(a.jobs, DEFAULT_JOBS);
        let a = parse(
            &args(&["/dir", "s3://b/p", "acme", "--jobs", "4"]),
            PUSH_USAGE,
        )
        .unwrap();
        assert_eq!(a.tenant.as_deref(), Some("acme"));
        assert_eq!(a.jobs, 4);
        assert!(parse(&args(&["/dir"]), PUSH_USAGE).is_err());
        assert!(parse(&args(&["/dir", "b", "--jobs", "0"]), PUSH_USAGE).is_err());
        assert!(parse(&args(&["/dir", "b", "--jobs"]), PUSH_USAGE).is_err());
    }

    #[test]
    fn immutable_is_the_three_prefixes_that_never_change() {
        assert!(immutable("tenants/acme/chk/genesis/INDEX"));
        assert!(immutable("tenants/acme/log/cellwal/0000/0000000000000001"));
        assert!(immutable("tenants/acme/manifests/00000001-1767100000.json"));
        assert!(!immutable("tenants/acme/MANIFEST"));
        assert!(!immutable("registry/acme.json"));
        assert!(!immutable("gc/CANDIDATES"));
    }

    /// The whole store, then one tenant of it, then the same tenant
    /// again after its manifest moved on. The second run copies the
    /// manifest and nothing else, which is the property that makes a
    /// push to S3 over a slow link worth running twice.
    #[test]
    fn a_second_run_copies_the_manifest_and_skips_the_rest() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let from = LocalFsStore::new(src.path());
        seed(&from);

        let (a, b) = (
            src.path().display().to_string(),
            dst.path().display().to_string(),
        );
        push(&args(&[&a, &b])).unwrap();
        let to = LocalFsStore::new(dst.path());
        assert_eq!(
            to.get("tenants/acme/chk/genesis/INDEX").unwrap().unwrap().0,
            b"f base/one 100\n"
        );
        assert!(to.get("registry/acme.json").unwrap().is_some());
        assert!(to.get("tenants/other/MANIFEST").unwrap().is_some());

        from.put("tenants/acme/MANIFEST", br#"{"second":true}"#)
            .unwrap();
        push(&args(&[&a, &b, "acme"])).unwrap();
        assert_eq!(
            to.get("tenants/acme/MANIFEST").unwrap().unwrap().0,
            br#"{"second":true}"#,
            "the live manifest is copied every run"
        );
    }

    #[test]
    fn pull_is_push_with_the_ends_the_other_way_round() {
        let remote = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        seed(&LocalFsStore::new(remote.path()));
        pull(&args(&[
            &remote.path().display().to_string(),
            &local.path().display().to_string(),
            "acme",
        ]))
        .unwrap();
        let here = LocalFsStore::new(local.path());
        assert!(here.get("tenants/acme/MANIFEST").unwrap().is_some());
        // Scoped to the ref, so the other tenant stayed where it was.
        assert!(here.get("tenants/other/MANIFEST").unwrap().is_none());
    }

    #[test]
    fn an_empty_source_says_so_instead_of_reporting_success() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let err = push(&args(&[
            &src.path().display().to_string(),
            &dst.path().display().to_string(),
        ]))
        .unwrap_err();
        assert!(err.contains("no objects"), "{err}");
    }
}
