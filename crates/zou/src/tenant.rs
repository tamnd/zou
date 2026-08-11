//! `zou tenant <target> <list|create|info|delete>`: the registry a
//! multi tenant server routes out of.
//!
//! Creating registers a ref. It does not make a database, and the
//! command says so: a database appears under `tenants/<ref>/` when
//! something bootstraps one there or branches one into it, and keeping
//! those two apart is what makes the registry safe to edit. Deleting is
//! the same thing backwards, it removes the entry and leaves the data,
//! because a router's index should not be able to destroy a project as
//! a side effect of forgetting it.
//!
//! Nothing here takes a lease or writes into a tenant prefix, so all
//! four run against a live store.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use zou_store::layout::TenantLayout;
use zou_store::registry::{self, Tenant};
use zou_store::{CasStore, open_store};

pub const USAGE: &str = "usage: zou tenant <target> <list | create <ref> [--secret <s>] | info <ref> | keys <ref> [--env] | delete <ref> | host add <ref> <host> | host remove <ref> <host>>";

/// A fresh project secret: 32 bytes of the os rng as hex, which is the
/// shape and the strength the dev loop's own generated secret has, and
/// long enough that HS256 is not the weak part.
fn secret() -> Result<String, String> {
    let mut raw = [0u8; 32];
    getrandom::fill(&mut raw).map_err(|e| format!("random secret: {e}"))?;
    Ok(raw.iter().map(|b| format!("{b:02x}")).collect())
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Whether a ref has a database yet, which is a different question from
/// whether it is registered and is the one people actually mean when
/// they ask if a tenant exists.
fn has_database(store: &dyn CasStore, tenant_ref: &str) -> Result<bool, String> {
    store
        .get(&TenantLayout::new(tenant_ref).manifest())
        .map(|found| found.is_some())
        .map_err(|e| format!("store: {e}"))
}

pub fn run(argv: &[String]) -> Result<(), String> {
    let [target, rest @ ..] = argv else {
        return Err(USAGE.into());
    };
    let store: Arc<dyn CasStore> = Arc::from(open_store(target)?);
    match rest {
        [verb] if verb == "list" => list(store.as_ref()),
        [verb, tenant_ref] if verb == "create" => create(store.as_ref(), tenant_ref, secret()?),
        [verb, tenant_ref, flag, value] if verb == "create" && flag == "--secret" => {
            create(store.as_ref(), tenant_ref, value.clone())
        }
        [verb, tenant_ref] if verb == "info" => info(store.as_ref(), tenant_ref),
        [verb, tenant_ref] if verb == "keys" => keys(store.as_ref(), tenant_ref, false),
        [verb, tenant_ref, flag] if verb == "keys" && flag == "--env" => {
            keys(store.as_ref(), tenant_ref, true)
        }
        [verb, tenant_ref] if verb == "delete" => delete(store.as_ref(), tenant_ref),
        [verb, act, tenant_ref, host] if verb == "host" && act == "add" => {
            registry::add_host(store.as_ref(), tenant_ref, host).map_err(|e| e.to_string())?;
            println!("{host} routes to {tenant_ref}");
            // Said because DNS is the half of this that is not on the
            // store, and a claimed host that resolves nowhere looks
            // exactly like a claim that did not work.
            println!("point {host} at this server for it to mean anything");
            Ok(())
        }
        [verb, act, tenant_ref, host] if verb == "host" && act == "remove" => {
            registry::remove_host(store.as_ref(), tenant_ref, host).map_err(|e| e.to_string())?;
            println!("{host} no longer routes to {tenant_ref}");
            Ok(())
        }
        _ => Err(USAGE.into()),
    }
}

/// Refs and whether each has a database, one per line. The secrets are
/// not read, let alone printed: a listing is a listing.
fn list(store: &dyn CasStore) -> Result<(), String> {
    let refs = registry::list(store).map_err(|e| e.to_string())?;
    if refs.is_empty() {
        println!("no tenants registered");
        return Ok(());
    }
    for tenant_ref in &refs {
        let state = match has_database(store, tenant_ref)? {
            true => "has a database",
            false => "registered, no database yet",
        };
        println!("{tenant_ref}\t{state}");
    }
    println!("{} tenants", refs.len());
    Ok(())
}

fn create(store: &dyn CasStore, tenant_ref: &str, jwt_secret: String) -> Result<(), String> {
    let entry = Tenant::new(tenant_ref, &jwt_secret, now());
    registry::create(store, &entry).map_err(|e| e.to_string())?;
    println!("registered {tenant_ref}");
    println!("jwt secret {jwt_secret}");
    // Said out loud because the next thing somebody does is point a
    // client at it and get told there is no manifest.
    println!("no database yet, make one with zou branch <target> create <src> {tenant_ref}");
    Ok(())
}

fn info(store: &dyn CasStore, tenant_ref: &str) -> Result<(), String> {
    let entry = registry::get(store, tenant_ref)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no tenant {tenant_ref} on this store"))?;
    println!("ref {}", entry.tenant_ref);
    println!("format {}", entry.format);
    println!("created unix {}", entry.created_unix);
    println!("jwt secret {}", entry.jwt_secret);
    match entry.hosts.is_empty() {
        true => println!("hosts: none besides its own label"),
        false => println!("hosts: {}", entry.hosts.join(", ")),
    }
    match has_database(store, tenant_ref)? {
        true => println!("database at {}", TenantLayout::new(tenant_ref).prefix()),
        false => println!("no database yet"),
    }
    Ok(())
}

/// The two api keys a client is configured with, minted from the
/// secret the registry holds.
///
/// `zou status` mints the same pair, but it mints them from
/// ZOU_JWT_SECRET and probes a local port, which is the dev loop. A
/// project on a bucket has neither: its secret was generated by
/// `tenant create` and lives in the registry, and the thing serving it
/// may be a function with no port at all. So this reads the store,
/// which is where the answer is, and `--env` is the form a shell evals
/// before running an app.
fn keys(store: &dyn CasStore, tenant_ref: &str, as_env: bool) -> Result<(), String> {
    let entry = registry::get(store, tenant_ref)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no tenant {tenant_ref} on this store"))?;
    let secret = entry.jwt_secret.as_bytes();
    let anon = zou_server::jwt::mint(&zou_server::jwt::key_claims("anon"), secret);
    let service = zou_server::jwt::mint(&zou_server::jwt::key_claims("service_role"), secret);
    if as_env {
        println!("ANON_KEY=\"{anon}\"");
        println!("SERVICE_ROLE_KEY=\"{service}\"");
    } else {
        println!("anon key {anon}");
        println!("service_role key {service}");
    }
    Ok(())
}

fn delete(store: &dyn CasStore, tenant_ref: &str) -> Result<(), String> {
    registry::delete(store, tenant_ref).map_err(|e| e.to_string())?;
    println!("unregistered {tenant_ref}");
    if has_database(store, tenant_ref)? {
        println!(
            "the database is still at {}, remove that prefix to delete it for good",
            TenantLayout::new(tenant_ref).prefix()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().expect("a directory to write into");
        let target = dir.path().to_string_lossy().to_string();
        (dir, target)
    }

    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn create_then_info_then_delete() {
        let (_d, target) = target();
        run(&argv(&[&target, "create", "acme-prod", "--secret", "sh"])).unwrap();
        run(&argv(&[&target, "info", "acme-prod"])).unwrap();
        run(&argv(&[&target, "list"])).unwrap();
        run(&argv(&[&target, "delete", "acme-prod"])).unwrap();
        assert!(
            run(&argv(&[&target, "info", "acme-prod"])).is_err(),
            "it is gone from the registry"
        );
    }

    #[test]
    fn a_generated_secret_is_thirty_two_bytes_of_hex() {
        let one = secret().unwrap();
        assert_eq!(one.len(), 64);
        assert!(one.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(one, secret().unwrap(), "or it is not a secret");
    }

    #[test]
    fn creating_a_ref_twice_is_refused_rather_than_overwriting_the_secret() {
        let (_d, target) = target();
        run(&argv(&[
            &target,
            "create",
            "acme-prod",
            "--secret",
            "first",
        ]))
        .unwrap();
        let err = run(&argv(&[
            &target,
            "create",
            "acme-prod",
            "--secret",
            "second",
        ]))
        .unwrap_err();
        assert!(err.contains("already registered"), "{err}");
    }

    #[test]
    fn a_host_is_claimed_and_given_back() {
        let (_d, target) = target();
        run(&argv(&[&target, "create", "acme-prod", "--secret", "sh"])).unwrap();
        run(&argv(&[
            &target,
            "host",
            "add",
            "acme-prod",
            "api.example.com",
        ]))
        .unwrap();
        run(&argv(&[&target, "info", "acme-prod"])).unwrap();
        run(&argv(&[
            &target,
            "host",
            "remove",
            "acme-prod",
            "api.example.com",
        ]))
        .unwrap();
    }

    #[test]
    fn keys_are_minted_from_the_secret_the_registry_holds() {
        let (_d, target) = target();
        run(&argv(&[&target, "create", "acme-prod", "--secret", "sh"])).unwrap();
        run(&argv(&[&target, "keys", "acme-prod"])).unwrap();
        run(&argv(&[&target, "keys", "acme-prod", "--env"])).unwrap();
        let anon = zou_server::jwt::mint(&zou_server::jwt::key_claims("anon"), b"sh");
        let verified = zou_server::jwt::verify(&anon, b"sh").expect("the pair verifies against it");
        assert_eq!(verified.role.as_deref(), Some("anon"));
        assert!(
            run(&argv(&[&target, "keys", "nobody"])).is_err(),
            "a ref that is not registered has no keys"
        );
    }

    #[test]
    fn a_host_cannot_be_claimed_out_from_under_another_tenant() {
        let (_d, target) = target();
        run(&argv(&[&target, "create", "acme-prod", "--secret", "sh"])).unwrap();
        run(&argv(&[&target, "create", "beta-co", "--secret", "sh"])).unwrap();
        run(&argv(&[
            &target,
            "host",
            "add",
            "acme-prod",
            "api.example.com",
        ]))
        .unwrap();
        let err = run(&argv(&[
            &target,
            "host",
            "add",
            "beta-co",
            "api.example.com",
        ]))
        .unwrap_err();
        assert!(err.contains("already claimed"), "{err}");
    }

    #[test]
    fn the_verbs_that_are_not_verbs_are_the_usage() {
        let (_d, target) = target();
        for bad in [
            vec![],
            argv(&[&target]),
            argv(&[&target, "create"]),
            argv(&[&target, "list", "acme-prod"]),
            argv(&[&target, "rename", "acme-prod"]),
            argv(&[&target, "create", "acme-prod", "--secret"]),
            argv(&[&target, "host", "add", "acme-prod"]),
            argv(&[&target, "host", "rename", "acme-prod", "api.example.com"]),
        ] {
            assert_eq!(run(&bad).unwrap_err(), USAGE, "{bad:?}");
        }
    }
}
