//! The tenant registry: who is on this store, and what the front door
//! needs to know about them before it has attached anything.
//!
//! One object per tenant, at `registry/<ref>.json`, rather than one
//! object listing all of them. The reason is what each operation costs.
//! Routing a request is a lookup of one ref and nothing else, and a
//! lookup of one ref against one object is a point GET whatever the
//! fleet size. Listing is an admin command, run by a person, and it
//! can afford to page through a prefix. A single list object would
//! invert that: it would make the common operation read every tenant,
//! and it would make two `tenant create` calls contend on one key.
//!
//! ```text
//! registry/<ref>.json    one tenant, everything the front door needs
//! tenants/<ref>/         that tenant's database and files
//! ```
//!
//! An entry is not the tenant. The tenant is the prefix, and the prefix
//! is self contained: copying it copies the project. An entry is a
//! pointer to one plus the handful of facts a router has to have before
//! it can decide whether to attach at all, which is why removing an
//! entry does not remove a database.
//!
//! ## Why the secret is in here
//!
//! Because the front door has to check an `apikey` before it attaches
//! anything. If the secret lived in the tenant's own database, then
//! finding out that a request was signed with the wrong key would first
//! require acquiring a lease, hydrating a manifest and starting a
//! session, and an unauthenticated request would be a lever for making
//! a server do all of that. Verification has to be cheaper than attach
//! or it is not verification, it is a queue.
//!
//! So the secret sits next to the ref, in the store, alongside the WAL
//! and the pages and the user files it protects. Anyone who can read
//! this object can already read the database it belongs to.

use serde::{Deserialize, Serialize};

use crate::cas::{CasError, CasStore};

/// Current entry format. A reader refuses anything newer rather than
/// misreading it, and accepts anything older.
pub const REGISTRY_FORMAT: u32 = 1;

/// The longest a ref may be, which is a DNS label's limit rather than a
/// key's. A ref becomes a hostname label under a wildcard domain, so
/// anything a label cannot hold is a ref that can be created and never
/// reached.
pub const REF_MAX: usize = 63;

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error(
        "registry format {found} is newer than this binary supports ({REGISTRY_FORMAT}), upgrade zou"
    )]
    FormatTooNew { found: u32 },
    #[error(
        "{tenant_ref:?} is not a usable ref: {why}. A ref is a hostname label, so it is 1 to {REF_MAX} of a to z, 0 to 9 and hyphen, starting and ending with a letter or a digit"
    )]
    BadRef { tenant_ref: String, why: String },
    #[error(
        "{host:?} is not a usable hostname: {why}. It is a DNS name, so it is labels of a to z, 0 to 9 and hyphen joined by dots"
    )]
    BadHost { host: String, why: String },
    #[error("tenant {tenant_ref} is already registered")]
    Exists { tenant_ref: String },
    #[error("{host} is already claimed by another tenant")]
    HostTaken { host: String },
    #[error("{host} belongs to {tenant_ref}, not to the tenant asking")]
    HostElsewhere { host: String, tenant_ref: String },
    #[error(
        "no tenant {tenant_ref} on this store, `zou tenant <target> list` shows what is registered"
    )]
    Missing { tenant_ref: String },
    #[error("invalid registry json for {tenant_ref}: {source}")]
    Json {
        tenant_ref: String,
        source: serde_json::Error,
    },
    #[error(transparent)]
    Store(#[from] CasError),
}

/// One tenant, as the front door sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tenant {
    pub format: u32,
    /// The ref, which is also the prefix its database lives under and
    /// the hostname label it answers on.
    #[serde(rename = "ref")]
    pub tenant_ref: String,
    pub created_unix: u64,
    /// The project's JWT secret, which every `apikey` and every bearer
    /// token is verified against. See the module docs for why it is
    /// here and not in the tenant's own database.
    pub jwt_secret: String,
    /// Hosts that route here besides the ref's own label under the
    /// serve domain. A project with its own domain in front of it is
    /// the reason this exists, and an empty list is the normal case.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hosts: Vec<String>,
    /// The pair this project's S3 protocol endpoint is asked with.
    ///
    /// Per tenant rather than per server, for the same reason the JWT
    /// secret is: a fleet answering one pair for every project on it
    /// would let whoever holds that pair sign for a project they were
    /// never given anything for, and the whole point of a key is that
    /// it opens one thing.
    ///
    /// Empty is a project with no pair, which is not a project with an
    /// open endpoint. A server told no pair says of every signed
    /// request that the access key it named is not one this project
    /// has, which is true.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub s3_access_key: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub s3_secret_key: String,
}

impl Tenant {
    pub fn new(tenant_ref: &str, jwt_secret: &str, created_unix: u64) -> Tenant {
        Tenant {
            format: REGISTRY_FORMAT,
            tenant_ref: tenant_ref.to_string(),
            created_unix,
            jwt_secret: jwt_secret.to_string(),
            hosts: Vec::new(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
        }
    }

    /// The pair, when there is one. Half a pair is not a pair: an entry
    /// carrying an access key and no secret is answered the same way as
    /// one carrying neither, because there is nothing to verify a
    /// signature against either way.
    pub fn s3(&self) -> Option<(&str, &str)> {
        match self.s3_access_key.is_empty() || self.s3_secret_key.is_empty() {
            true => None,
            false => Some((&self.s3_access_key, &self.s3_secret_key)),
        }
    }

    pub fn to_json(&self) -> Vec<u8> {
        let mut out = serde_json::to_vec_pretty(self).expect("a tenant entry serializes");
        out.push(b'\n');
        out
    }

    fn from_json(tenant_ref: &str, data: &[u8]) -> Result<Tenant, RegistryError> {
        let entry: Tenant = serde_json::from_slice(data).map_err(|source| RegistryError::Json {
            tenant_ref: tenant_ref.to_string(),
            source,
        })?;
        match entry.format > REGISTRY_FORMAT {
            true => Err(RegistryError::FormatTooNew {
                found: entry.format,
            }),
            false => Ok(entry),
        }
    }
}

/// Where one tenant's entry lives.
pub fn entry_key(tenant_ref: &str) -> String {
    format!("registry/{tenant_ref}.json")
}

/// The whole registry prefix, which `list` pages through.
pub fn entries_prefix() -> String {
    "registry/".to_string()
}

/// A ref has to survive being a key component and a hostname label, and
/// the label is the tighter of the two, so that is the rule.
///
/// It is checked on the way in rather than on the way out. A ref that
/// cannot be routed to is a tenant somebody paid to create and can
/// never reach, and finding that out at the first request is finding it
/// out from the wrong end.
pub fn check_ref(tenant_ref: &str) -> Result<(), RegistryError> {
    let bad = |why: &str| RegistryError::BadRef {
        tenant_ref: tenant_ref.to_string(),
        why: why.to_string(),
    };
    if tenant_ref.is_empty() {
        return Err(bad("it is empty"));
    }
    if tenant_ref.len() > REF_MAX {
        return Err(bad(&format!("it is {} characters", tenant_ref.len())));
    }
    if let Some(c) = tenant_ref
        .chars()
        .find(|c| !c.is_ascii_lowercase() && !c.is_ascii_digit() && *c != '-')
    {
        return Err(bad(&format!("{c:?} is not allowed in one")));
    }
    // A leading or trailing hyphen is not a label, and a leading one is
    // also an argument that looks like a flag.
    if tenant_ref.starts_with('-') || tenant_ref.ends_with('-') {
        return Err(bad("it starts or ends with a hyphen"));
    }
    Ok(())
}

/// Register a tenant, refusing to take a ref somebody already has.
///
/// The write is conditional on the object not existing, so two creates
/// of the same ref against the same store cannot both win: one of them
/// is told the ref is taken. That guarantee is per key, which is the
/// other half of why this is one object per tenant.
pub fn create(store: &dyn CasStore, entry: &Tenant) -> Result<(), RegistryError> {
    check_ref(&entry.tenant_ref)?;
    match store.put_if_absent(&entry_key(&entry.tenant_ref), &entry.to_json()) {
        Ok(_) => Ok(()),
        Err(CasError::Conflict { .. }) | Err(CasError::AlreadyExists { .. }) => {
            Err(RegistryError::Exists {
                tenant_ref: entry.tenant_ref.clone(),
            })
        }
        Err(e) => Err(e.into()),
    }
}

/// One tenant, or `None` when the store has never heard of it. This is
/// the lookup on the routing path and it is one GET.
pub fn get(store: &dyn CasStore, tenant_ref: &str) -> Result<Option<Tenant>, RegistryError> {
    let Some((data, _)) = store.get(&entry_key(tenant_ref))? else {
        return Ok(None);
    };
    Tenant::from_json(tenant_ref, &data).map(Some)
}

/// Every ref on the store, sorted. Refs rather than entries, because
/// this is a listing and reading every secret to print a list of names
/// is a thing not to do.
pub fn list(store: &dyn CasStore) -> Result<Vec<String>, RegistryError> {
    let prefix = entries_prefix();
    let mut refs: Vec<String> = store
        .list(&prefix)?
        .iter()
        .filter_map(|key| {
            key.strip_prefix(&prefix)
                .and_then(|name| name.strip_suffix(".json"))
                .map(str::to_string)
        })
        .collect();
    refs.sort();
    Ok(refs)
}

/// The longest a hostname may be, the DNS limit.
pub const HOST_MAX: usize = 253;

/// One custom hostname, pointing at the tenant that claimed it.
///
/// A separate object rather than a scan of every entry's `hosts` list,
/// for the same reason the entries are one per tenant: resolving a host
/// is on the request path and has to be one GET, and claiming one has
/// to be a conditional write so that two projects cannot both end up
/// owning `api.example.com`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Alias {
    pub format: u32,
    pub host: String,
    #[serde(rename = "ref")]
    pub tenant_ref: String,
}

/// Where one custom hostname lives. Not under `registry/`, so that
/// listing the tenants cannot ever turn a hostname into something that
/// looks like a ref.
pub fn host_key(host: &str) -> String {
    format!("hosts/{host}.json")
}

/// A hostname is checked the way a ref is, and for the same reason: a
/// name that DNS cannot carry is a name a project can claim and never
/// be reached on.
///
/// It must have a dot in it. A bare label under no domain would be
/// ambiguous with the labels a wildcard domain already routes, and
/// nothing good comes of a custom host that shadows a ref.
pub fn check_host(host: &str) -> Result<(), RegistryError> {
    let bad = |why: &str| RegistryError::BadHost {
        host: host.to_string(),
        why: why.to_string(),
    };
    if host.is_empty() || host.len() > HOST_MAX {
        return Err(bad("it is empty or longer than a hostname may be"));
    }
    if !host.contains('.') {
        return Err(bad("it has no dot in it, so it is a label and not a host"));
    }
    for label in host.split('.') {
        if label.is_empty() || label.len() > REF_MAX {
            return Err(bad("one of its labels is empty or too long"));
        }
        if label.starts_with('-') || label.ends_with('-') {
            return Err(bad("one of its labels starts or ends with a hyphen"));
        }
        if let Some(c) = label
            .chars()
            .find(|c| !c.is_ascii_lowercase() && !c.is_ascii_digit() && *c != '-')
        {
            return Err(bad(&format!("{c:?} is not allowed in a hostname")));
        }
    }
    Ok(())
}

/// Point a hostname at a tenant.
///
/// Conditional on the hostname being unclaimed, so the first project to
/// ask for `api.example.com` is the one that has it, and a second is
/// told rather than quietly taking it over. Asking twice for a host a
/// tenant already owns is not an error, because an operator rerunning a
/// script should not have to care.
///
/// The alias is written before the entry is updated. Both orders leave
/// something behind on a crash and this is the harmless one: an alias
/// with no mention in the entry still routes, while an entry claiming a
/// host with no alias would route nowhere and read as a bug.
pub fn add_host(store: &dyn CasStore, tenant_ref: &str, host: &str) -> Result<(), RegistryError> {
    check_host(host)?;
    let Some(mut entry) = get(store, tenant_ref)? else {
        return Err(RegistryError::Missing {
            tenant_ref: tenant_ref.to_string(),
        });
    };
    let alias = Alias {
        format: REGISTRY_FORMAT,
        host: host.to_string(),
        tenant_ref: tenant_ref.to_string(),
    };
    let mut body = serde_json::to_vec_pretty(&alias).expect("an alias serializes");
    body.push(b'\n');
    match store.put_if_absent(&host_key(host), &body) {
        Ok(_) => {}
        Err(CasError::Conflict { .. }) | Err(CasError::AlreadyExists { .. }) => {
            match host_ref(store, host)? {
                Some(owner) if owner == tenant_ref => {}
                _ => {
                    return Err(RegistryError::HostTaken {
                        host: host.to_string(),
                    });
                }
            }
        }
        Err(e) => return Err(e.into()),
    }
    if !entry.hosts.iter().any(|h| h == host) {
        entry.hosts.push(host.to_string());
        entry.hosts.sort();
        store.put(&entry_key(tenant_ref), &entry.to_json())?;
    }
    Ok(())
}

/// Stop a hostname routing anywhere, refusing to take one off a tenant
/// that does not own it.
pub fn remove_host(
    store: &dyn CasStore,
    tenant_ref: &str,
    host: &str,
) -> Result<(), RegistryError> {
    match host_ref(store, host)? {
        Some(owner) if owner == tenant_ref => store.delete(&host_key(host))?,
        Some(owner) => {
            return Err(RegistryError::HostElsewhere {
                host: host.to_string(),
                tenant_ref: owner,
            });
        }
        None => {}
    }
    if let Some(mut entry) = get(store, tenant_ref)?
        && entry.hosts.iter().any(|h| h == host)
    {
        entry.hosts.retain(|h| h != host);
        store.put(&entry_key(tenant_ref), &entry.to_json())?;
    }
    Ok(())
}

/// Give a tenant an S3 pair, or replace the one it has.
///
/// Replacing is what rotating is, and it takes effect the next time
/// something reads the entry rather than at once, because a server that
/// has the project attached is holding the old pair in its config. That
/// is the same lag a changed JWT secret has and it is a property of the
/// registry being the source rather than a cache of one.
pub fn set_s3(
    store: &dyn CasStore,
    tenant_ref: &str,
    access: &str,
    secret: &str,
) -> Result<(), RegistryError> {
    let Some(mut entry) = get(store, tenant_ref)? else {
        return Err(RegistryError::Missing {
            tenant_ref: tenant_ref.to_string(),
        });
    };
    entry.s3_access_key = access.to_string();
    entry.s3_secret_key = secret.to_string();
    store.put(&entry_key(tenant_ref), &entry.to_json())?;
    Ok(())
}

/// Which tenant a hostname belongs to, in one GET. This is the routing
/// path for every project on its own domain.
pub fn host_ref(store: &dyn CasStore, host: &str) -> Result<Option<String>, RegistryError> {
    let Some((data, _)) = store.get(&host_key(host))? else {
        return Ok(None);
    };
    let alias: Alias = serde_json::from_slice(&data).map_err(|source| RegistryError::Json {
        tenant_ref: host.to_string(),
        source,
    })?;
    match alias.format > REGISTRY_FORMAT {
        true => Err(RegistryError::FormatTooNew {
            found: alias.format,
        }),
        false => Ok(Some(alias.tenant_ref)),
    }
}

/// Take a tenant off the registry.
///
/// This removes the entry and nothing else. The database and the files
/// stay where they are, under `tenants/<ref>/`, because unregistering
/// is the reversible half and deleting a project's data is not
/// something a router's index should be able to do as a side effect.
/// Removing a tenant for good is removing that prefix, and it is a
/// separate act on purpose.
pub fn delete(store: &dyn CasStore, tenant_ref: &str) -> Result<(), RegistryError> {
    if get(store, tenant_ref)?.is_none() {
        return Err(RegistryError::Missing {
            tenant_ref: tenant_ref.to_string(),
        });
    }
    // The aliases go with it. They are not the project's data, they
    // are pointers at a pointer, and one left behind is a hostname
    // nobody can reclaim because the tenant that owned it is gone.
    for host in get(store, tenant_ref)?
        .map(|entry| entry.hosts)
        .unwrap_or_default()
    {
        if host_ref(store, &host)?.as_deref() == Some(tenant_ref) {
            store.delete(&host_key(&host))?;
        }
    }
    store.delete(&entry_key(tenant_ref))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::MemStore;

    fn tenant(r: &str) -> Tenant {
        Tenant::new(
            r,
            "super-secret-jwt-token-with-at-least-32-characters-long",
            1_767_100_000,
        )
    }

    #[test]
    fn an_entry_round_trips_and_sits_where_the_layout_says() {
        let store = MemStore::new();
        create(&store, &tenant("acme-prod")).unwrap();
        assert_eq!(store.list("").unwrap(), vec!["registry/acme-prod.json"]);
        assert_eq!(get(&store, "acme-prod").unwrap(), Some(tenant("acme-prod")));
    }

    #[test]
    fn an_entry_written_before_s3_existed_still_reads() {
        // What an older zou wrote, which is every entry on every store
        // that already has one. It has no pair, and having no pair is a
        // thing an entry is allowed to be rather than a parse failure.
        let store = MemStore::new();
        let older = br#"{"format":1,"ref":"acme-prod","created_unix":1,"jwt_secret":"s"}"#;
        store.put(&entry_key("acme-prod"), older).unwrap();
        let entry = get(&store, "acme-prod").unwrap().expect("it reads");
        assert_eq!(entry.s3(), None);
        assert!(
            !String::from_utf8(entry.to_json()).unwrap().contains("s3"),
            "and writing it back does not invent one"
        );
    }

    #[test]
    fn half_a_pair_is_not_a_pair() {
        let store = MemStore::new();
        create(&store, &tenant("acme-prod")).unwrap();
        set_s3(&store, "acme-prod", "an-access-key", "").unwrap();
        assert_eq!(get(&store, "acme-prod").unwrap().unwrap().s3(), None);
        set_s3(&store, "acme-prod", "an-access-key", "a-secret").unwrap();
        assert_eq!(
            get(&store, "acme-prod").unwrap().unwrap().s3(),
            Some(("an-access-key", "a-secret"))
        );
        let err = set_s3(&store, "nobody", "a", "b").unwrap_err();
        assert!(matches!(err, RegistryError::Missing { .. }));
    }

    #[test]
    fn a_ref_nobody_registered_is_none_rather_than_an_error() {
        // The routing path asks about refs that do not exist every time
        // somebody types a url wrong, and that is a 404 rather than a
        // thing to log.
        assert!(get(&MemStore::new(), "nobody").unwrap().is_none());
    }

    #[test]
    fn two_creates_of_one_ref_cannot_both_win() {
        let store = MemStore::new();
        create(&store, &tenant("acme-prod")).unwrap();
        let err = create(&store, &tenant("acme-prod")).unwrap_err();
        assert!(matches!(err, RegistryError::Exists { .. }));
    }

    #[test]
    fn listing_gives_refs_in_order_and_reads_no_secrets() {
        let store = MemStore::new();
        for r in ["zed", "acme-prod", "m1"] {
            create(&store, &tenant(r)).unwrap();
        }
        assert_eq!(list(&store).unwrap(), ["acme-prod", "m1", "zed"]);
    }

    #[test]
    fn deleting_takes_the_entry_and_leaves_the_database() {
        let store = MemStore::new();
        create(&store, &tenant("acme-prod")).unwrap();
        store
            .put("tenants/acme-prod/MANIFEST", b"not really a manifest")
            .unwrap();
        delete(&store, "acme-prod").unwrap();
        assert!(get(&store, "acme-prod").unwrap().is_none());
        assert!(
            store.get("tenants/acme-prod/MANIFEST").unwrap().is_some(),
            "unregistering is the reversible half"
        );
        let err = delete(&store, "acme-prod").unwrap_err();
        assert!(matches!(err, RegistryError::Missing { .. }));
    }

    #[test]
    fn a_ref_has_to_be_a_hostname_label() {
        for good in ["a", "acme-prod", "m1", &"x".repeat(REF_MAX)] {
            check_ref(good).expect(good);
        }
        for bad in [
            "",
            "Acme",
            "acme_prod",
            "acme.prod",
            "-acme",
            "acme-",
            "acme/prod",
            "../etc",
            &"x".repeat(REF_MAX + 1),
        ] {
            assert!(check_ref(bad).is_err(), "{bad:?} should be refused");
        }
    }

    #[test]
    fn a_ref_that_is_not_a_label_is_refused_before_it_is_written() {
        let store = MemStore::new();
        let err = create(&store, &tenant("../etc/passwd")).unwrap_err();
        assert!(matches!(err, RegistryError::BadRef { .. }));
        assert!(
            store.list("").unwrap().is_empty(),
            "nothing reaches the store, so nothing has to be cleaned up"
        );
    }

    #[test]
    fn an_entry_from_the_future_is_refused_rather_than_misread() {
        let store = MemStore::new();
        let mut ahead = tenant("acme-prod");
        ahead.format = REGISTRY_FORMAT + 1;
        store
            .put(&entry_key("acme-prod"), &ahead.to_json())
            .unwrap();
        let err = get(&store, "acme-prod").unwrap_err();
        assert!(matches!(err, RegistryError::FormatTooNew { .. }));
    }

    #[test]
    fn a_host_points_at_the_tenant_that_claimed_it() {
        let store = MemStore::new();
        create(&store, &tenant("acme-prod")).unwrap();
        add_host(&store, "acme-prod", "api.example.com").unwrap();
        assert_eq!(
            host_ref(&store, "api.example.com").unwrap().as_deref(),
            Some("acme-prod")
        );
        assert_eq!(
            get(&store, "acme-prod").unwrap().unwrap().hosts,
            vec!["api.example.com".to_string()],
            "and the entry says which hosts it has, so they can be cleaned up"
        );
    }

    #[test]
    fn the_first_tenant_to_ask_for_a_host_keeps_it() {
        let store = MemStore::new();
        create(&store, &tenant("acme-prod")).unwrap();
        create(&store, &tenant("beta-co")).unwrap();
        add_host(&store, "acme-prod", "api.example.com").unwrap();
        let err = add_host(&store, "beta-co", "api.example.com").unwrap_err();
        assert!(matches!(err, RegistryError::HostTaken { .. }), "{err}");
        assert_eq!(
            host_ref(&store, "api.example.com").unwrap().as_deref(),
            Some("acme-prod"),
            "and it still points where it did"
        );
    }

    #[test]
    fn claiming_a_host_twice_is_not_an_error_for_the_tenant_that_has_it() {
        let store = MemStore::new();
        create(&store, &tenant("acme-prod")).unwrap();
        add_host(&store, "acme-prod", "api.example.com").unwrap();
        add_host(&store, "acme-prod", "api.example.com").unwrap();
        assert_eq!(
            get(&store, "acme-prod").unwrap().unwrap().hosts.len(),
            1,
            "an operator rerunning a script should not have to care"
        );
    }

    #[test]
    fn a_host_can_be_taken_back_only_by_its_owner() {
        let store = MemStore::new();
        create(&store, &tenant("acme-prod")).unwrap();
        create(&store, &tenant("beta-co")).unwrap();
        add_host(&store, "acme-prod", "api.example.com").unwrap();
        let err = remove_host(&store, "beta-co", "api.example.com").unwrap_err();
        assert!(matches!(err, RegistryError::HostElsewhere { .. }), "{err}");
        remove_host(&store, "acme-prod", "api.example.com").unwrap();
        assert!(host_ref(&store, "api.example.com").unwrap().is_none());
        assert!(get(&store, "acme-prod").unwrap().unwrap().hosts.is_empty());
    }

    #[test]
    fn unregistering_a_tenant_frees_its_hosts() {
        let store = MemStore::new();
        create(&store, &tenant("acme-prod")).unwrap();
        add_host(&store, "acme-prod", "api.example.com").unwrap();
        delete(&store, "acme-prod").unwrap();
        assert!(
            host_ref(&store, "api.example.com").unwrap().is_none(),
            "or the name is claimed forever by a tenant that is not there"
        );
        create(&store, &tenant("beta-co")).unwrap();
        add_host(&store, "beta-co", "api.example.com").unwrap();
    }

    #[test]
    fn a_host_that_dns_could_not_carry_is_refused() {
        let store = MemStore::new();
        create(&store, &tenant("acme-prod")).unwrap();
        for host in [
            "",
            "nodot",
            "UPPER.example.com",
            "-lead.example.com",
            "trail-.example.com",
            "two..dots.com",
            "under_score.example.com",
        ] {
            let err = add_host(&store, "acme-prod", host).unwrap_err();
            assert!(
                matches!(err, RegistryError::BadHost { .. }),
                "{host}: {err}"
            );
        }
    }

    #[test]
    fn a_host_cannot_be_claimed_for_a_tenant_that_is_not_registered() {
        let store = MemStore::new();
        let err = add_host(&store, "acme-prod", "api.example.com").unwrap_err();
        assert!(matches!(err, RegistryError::Missing { .. }), "{err}");
    }

    #[test]
    fn a_hostname_is_not_a_ref_when_the_tenants_are_listed() {
        let store = MemStore::new();
        create(&store, &tenant("acme-prod")).unwrap();
        add_host(&store, "acme-prod", "api.example.com").unwrap();
        assert_eq!(list(&store).unwrap(), vec!["acme-prod".to_string()]);
    }
}
