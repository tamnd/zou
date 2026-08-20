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

pub const USAGE: &str = "usage: zou tenant <target> <list | create <ref> [--secret <s>] | info <ref> | keys <ref> [--env] | s3 <ref> [--rotate] | delete <ref> | host add <ref> <host> | host remove <ref> <host>>";

/// A fresh project secret: 32 bytes of the os rng as hex, which is the
/// shape and the strength the dev loop's own generated secret has, and
/// long enough that HS256 is not the weak part.
fn secret() -> Result<String, String> {
    random(32)
}

/// n bytes of the os rng as hex.
fn random(n: usize) -> Result<String, String> {
    let mut raw = vec![0u8; n];
    getrandom::fill(&mut raw).map_err(|e| format!("random secret: {e}"))?;
    Ok(raw.iter().map(|b| format!("{b:02x}")).collect())
}

/// A fresh S3 pair, in the shape a Supabase project's is: an access key
/// of 32 hex characters and a secret of 64. The lengths are what a
/// client's configuration field expects to be given, and both come out
/// of the same rng the project secret does.
fn s3_pair() -> Result<(String, String), String> {
    Ok((random(16)?, random(32)?))
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
        [verb, tenant_ref] if verb == "s3" => s3(store.as_ref(), tenant_ref, false),
        [verb, tenant_ref, flag] if verb == "s3" && flag == "--rotate" => {
            s3(store.as_ref(), tenant_ref, true)
        }
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
    let mut entry = Tenant::new(tenant_ref, &jwt_secret, now());
    // Every project gets its own S3 pair at creation, because a project
    // whose S3 endpoint refuses everything until somebody runs a second
    // command is a project whose storage clients do not work and no
    // error says why.
    let (access, secret) = s3_pair()?;
    entry.s3_access_key = access.clone();
    entry.s3_secret_key = secret.clone();
    registry::create(store, &entry).map_err(|e| e.to_string())?;
    println!("registered {tenant_ref}");
    println!("jwt secret {jwt_secret}");
    println!("s3 access key {access}");
    println!("s3 secret key {secret}");
    // Said out loud because the next thing somebody does is point a
    // client at it and get told there is no manifest.
    println!("no database yet, make one with zou branch <target> create <src> {tenant_ref}");
    Ok(())
}

fn info(store: &dyn CasStore, tenant_ref: &str) -> Result<(), String> {
    let entry = registry::get(store, tenant_ref)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| {
            format!("no tenant {tenant_ref} on this store, `list` shows what is registered")
        })?;
    println!("ref {}", entry.tenant_ref);
    println!("format {}", entry.format);
    println!("created unix {}", entry.created_unix);
    println!("jwt secret {}", entry.jwt_secret);
    match entry.s3() {
        Some((access, secret)) => {
            println!("s3 access key {access}");
            println!("s3 secret key {secret}");
        }
        // Said out loud rather than left blank, because an S3 client
        // being told its key is not one this project has looks the same
        // as a wrong key typed in.
        None => println!("no s3 pair, make one with zou tenant <target> s3 {tenant_ref}"),
    }
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

/// One legacy format project key: HS256 over the claim set Supabase
/// puts in an anon or service_role key, iss, role, iat, and a ten year
/// exp.
///
/// The server has this same twenty lines in `zou_server::jwt`, which is
/// where tokens are verified and where every other format lives. It is
/// not called from here because that crate supervises a postmaster and
/// so is compiled on unix only, while `zou tenant` is a store tool that
/// works wherever a bucket does. The test below mints with this and
/// verifies with that one, so a drift between them fails rather than
/// ships.
fn mint(role: &str, secret: &[u8]) -> String {
    use base64ct::{Base64UrlUnpadded, Encoding};
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    let iat = now();
    let claims = serde_json::json!({
        "iss": "zou",
        "role": role,
        "iat": iat,
        "exp": iat + 10 * 365 * 24 * 3600,
    });
    let header = Base64UrlUnpadded::encode_string(br#"{"alg":"HS256","typ":"JWT"}"#);
    let payload = Base64UrlUnpadded::encode_string(claims.to_string().as_bytes());
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(header.as_bytes());
    mac.update(b".");
    mac.update(payload.as_bytes());
    let sig = Base64UrlUnpadded::encode_string(&mac.finalize().into_bytes());
    format!("{header}.{payload}.{sig}")
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
        .ok_or_else(|| {
            format!("no tenant {tenant_ref} on this store, `list` shows what is registered")
        })?;
    let secret = entry.jwt_secret.as_bytes();
    let anon = mint("anon", secret);
    let service = mint("service_role", secret);
    if as_env {
        println!("ANON_KEY=\"{anon}\"");
        println!("SERVICE_ROLE_KEY=\"{service}\"");
    } else {
        println!("anon key {anon}");
        println!("service_role key {service}");
    }
    // The S3 pair is read rather than minted, and it is printed here
    // because a client configured against a project needs all of these
    // together. The names are the ones `supabase status -o env` uses.
    if let Some((access, s3_secret)) = entry.s3() {
        if as_env {
            println!("S3_PROTOCOL_ACCESS_KEY_ID=\"{access}\"");
            println!("S3_PROTOCOL_ACCESS_KEY_SECRET=\"{s3_secret}\"");
            println!("S3_PROTOCOL_REGION=\"{}\"", zou_server_region());
        } else {
            println!("s3 access key {access}");
            println!("s3 secret key {s3_secret}");
            println!("s3 region {}", zou_server_region());
        }
    }
    Ok(())
}

/// Where a served project says it is. One constant rather than a
/// setting, because nothing on this side of the store knows which
/// region a bucket is in and every S3 client assumes this one when it
/// is not told otherwise.
///
/// Spelled out here rather than taken from `zou_server::s3::REGION`
/// because that crate supervises a postmaster and is built on unix
/// only, while `zou tenant` is a store tool that runs wherever a bucket
/// does. The test below is what keeps the two the same.
fn zou_server_region() -> &'static str {
    "us-east-1"
}

/// Give a tenant an S3 pair, and refuse to replace one it already has
/// unless that is what was asked for. Rotating is a thing an operator
/// means to do, not a thing a rerun of a setup script does.
fn s3(store: &dyn CasStore, tenant_ref: &str, rotate: bool) -> Result<(), String> {
    let entry = registry::get(store, tenant_ref)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| {
            format!("no tenant {tenant_ref} on this store, `list` shows what is registered")
        })?;
    if let Some((access, _)) = entry.s3()
        && !rotate
    {
        return Err(format!(
            "{tenant_ref} already has an s3 pair, access key {access}. Replace it with zou tenant <target> s3 {tenant_ref} --rotate"
        ));
    }
    let (access, secret) = s3_pair()?;
    registry::set_s3(store, tenant_ref, &access, &secret).map_err(|e| e.to_string())?;
    println!("s3 access key {access}");
    println!("s3 secret key {secret}");
    // Said because the pair a running server holds came out of the
    // entry when it attached the project, and it goes on holding it.
    if rotate {
        println!("the old pair still works until this project is next attached");
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
    fn a_project_is_created_with_an_s3_pair_and_can_rotate_it() {
        let (_d, target) = target();
        run(&argv(&[&target, "create", "acme-prod", "--secret", "sh"])).unwrap();
        let store = zou_store::open_store(&target).unwrap();
        let first = registry::get(store.as_ref(), "acme-prod").unwrap().unwrap();
        let (access, secret) = first.s3().expect("created with a pair");
        assert_eq!((access.len(), secret.len()), (32, 64));
        let err = run(&argv(&[&target, "s3", "acme-prod"])).unwrap_err();
        assert!(err.contains("already has an s3 pair"), "{err}");
        run(&argv(&[&target, "s3", "acme-prod", "--rotate"])).unwrap();
        let second = registry::get(store.as_ref(), "acme-prod").unwrap().unwrap();
        assert_ne!(second.s3(), first.s3(), "rotating rotates");
        assert!(
            run(&argv(&[&target, "s3", "nobody"])).is_err(),
            "a ref that is not registered has nothing to rotate"
        );
    }

    #[test]
    fn the_region_here_is_the_one_the_server_verifies_against() {
        #[cfg(unix)]
        assert_eq!(zou_server_region(), zou_server::s3::REGION);
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
        // What the printed keys are worth is whether the server takes
        // them, so mint one here and verify it there.
        #[cfg(unix)]
        {
            let anon = mint("anon", b"sh");
            let verified =
                zou_server::jwt::verify(&anon, b"sh").expect("the server takes what this mints");
            assert_eq!(verified.role.as_deref(), Some("anon"));
            assert!(
                zou_server::jwt::verify(&anon, b"another project's secret").is_err(),
                "a key is only a key for the project whose secret signed it"
            );
        }
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
