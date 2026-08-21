//! `zou shard <target> <ref> split|merge`: double or halve a tenant's
//! page shard count.
//!
//! Both directions are metadata only, one CAS on the tenant manifest,
//! whatever the database size. The shards the change creates serve
//! their ancestors' layers in place until compaction separates them,
//! so the call returns in the time of a few manifest round trips and
//! nothing else moves.

use zou_store::open_store;

pub const USAGE: &str = "usage: zou shard <target> <ref> split|merge";

pub fn run(argv: &[String]) -> Result<(), String> {
    let (target, tenant_ref, verb) = match argv {
        [target, tenant_ref, verb] => (target, tenant_ref, verb.as_str()),
        _ => return Err(USAGE.into()),
    };
    let store = open_store(target)?;
    let manifest = match verb {
        "split" => zou_store::split(&*store, tenant_ref),
        "merge" => zou_store::merge(&*store, tenant_ref),
        _ => return Err(USAGE.into()),
    }
    .map_err(|e| e.to_string())?;
    let change = manifest
        .shard_history
        .last()
        .ok_or("a resize always leaves a lineage entry")?;
    say!(
        "{verb} {tenant_ref} from {} to {} shards, floor {:#X}",
        change.from,
        change.to,
        change.at.0
    );
    Ok(())
}
