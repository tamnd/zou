//! A project's function secrets, and the one object they live in.
//!
//! On a laptop the secrets are `supabase/functions/.env`, and that file
//! is read straight off the disk beside the functions. A deployed
//! project has no disk beside it, so its secrets are an object in its
//! own prefix:
//!
//! ```text
//! tenants/<ref>/functions/SECRETS
//! ```
//!
//! and the object is sealed. The whole point of putting a database on
//! object storage is that the storage is somebody else's, so a bucket
//! is a thing that can be copied without the copy being noticed, and
//! secrets written in the clear next to the data they unlock would make
//! that copy worth having. What is in the object is a nonce and a
//! ciphertext, and the key is not in the store at all.
//!
//! # Where the key comes from
//!
//! `ZOU_SECRET_KEY`, thirty two bytes as base64 or hex, or
//! `ZOU_SECRET_KEY_FILE` naming a file holding the same thing, and
//! `zou secrets key` prints a fresh one. There is deliberately no
//! `--secret-key` flag: an argument is in `ps` output and in the shell
//! history of whoever ran it, and a key that leaks that easily is not
//! one worth encrypting with.
//!
//! The root key is never used to encrypt anything. Every tenant gets
//! its own key, derived as
//!
//! ```text
//! HMAC-SHA256(root, "zou/functions/secrets/1/<ref>")
//! ```
//!
//! which is one HKDF expand step with the label spelled out. That means
//! a ciphertext lifted out of one project's prefix and dropped into
//! another's does not open, and the same label goes in as the
//! associated data, so it does not open even if somebody rewrites the
//! key it sits under.
//!
//! # What is sealed
//!
//! The whole map at once, names and values together, rather than each
//! value on its own. Names leak in a per value scheme, and the names
//! are half of what an attacker wants: `STRIPE_SECRET_KEY` tells them
//! what the project is worth breaking into. The cost is that reading
//! one secret means reading all of them, and there is no version of
//! this where a node is holding some of a project's environment.
//!
//! A node with no key does not serve a project that has secrets. Not
//! serving is the honest answer, because the alternative is a function
//! running without the environment it was written against, which is a
//! function calling somebody else's api with an empty token.

use std::collections::BTreeMap;

use base64ct::{Base64, Encoding};
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Nonce};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use zou_store::CasStore;
use zou_store::cas::CasError;
use zou_store::layout::TenantLayout;

/// The format of the sealed object.
pub const VERSION: u32 = 1;

/// Upstream's prefix, which a project may not set for itself: the four
/// variables that name a project are the server's to fill in, and a
/// function that could be handed a different `SUPABASE_URL` than the
/// one it answers on is a function nobody can reason about.
const RESERVED: &str = "SUPABASE_";

/// The label a tenant's key is derived under, and the associated data
/// the seal is bound to. The version is in it, so a later format is a
/// different key rather than the same key used two ways.
fn label(tenant_ref: &str) -> String {
    format!("zou/functions/secrets/{VERSION}/{tenant_ref}")
}

/// The root key a fleet is run with.
///
/// Held as bytes and nothing else, printed by nothing, and overwritten
/// when it is dropped. That last part is best effort rather than a
/// guarantee: a `String` this was parsed out of has already been
/// through the allocator, and a process that can be dumped has been
/// lost already.
pub struct Key([u8; 32]);

impl Drop for Key {
    fn drop(&mut self) {
        for byte in &mut self.0 {
            // Volatile so the write is not the dead store the optimiser
            // is entitled to think it is.
            unsafe { std::ptr::write_volatile(byte, 0) };
        }
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
    }
}

impl std::fmt::Debug for Key {
    /// Never the bytes. This exists so a struct holding one can derive
    /// `Debug` without the key reaching a log line.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Key(..)")
    }
}

impl Key {
    /// The key this process was started with, or None when it was
    /// started with none, which is every project that has no secrets.
    ///
    /// `ZOU_SECRET_KEY_FILE` first, because a file is what a secret
    /// manager mounts and an environment variable is what a person
    /// exports, and the mounted one should win where both are set.
    pub fn from_env() -> Result<Option<Key>, String> {
        if let Some(path) = var("ZOU_SECRET_KEY_FILE") {
            let text = std::fs::read_to_string(&path)
                .map_err(|e| format!("read the key at {path}: {e}"))?;
            return Key::parse(text.trim()).map(Some);
        }
        match var("ZOU_SECRET_KEY") {
            Some(text) => Key::parse(text.trim()).map(Some),
            None => Ok(None),
        }
    }

    /// Thirty two bytes, written as base64 or as hex, because an
    /// operator pasting a key from a password manager should not have
    /// to know which one this program wanted.
    pub fn parse(text: &str) -> Result<Key, String> {
        let mut raw = [0u8; 32];
        if text.len() == 64
            && text.chars().all(|c| c.is_ascii_hexdigit())
            && let Ok(bytes) = hex(text)
        {
            raw.copy_from_slice(&bytes);
            return Ok(Key(raw));
        }
        let bytes = Base64::decode_vec(text)
            .map_err(|_| "the secret key is not base64 or hex".to_string())?;
        if bytes.len() != 32 {
            return Err(format!(
                "the secret key is {} bytes and has to be 32",
                bytes.len()
            ));
        }
        raw.copy_from_slice(&bytes);
        Ok(Key(raw))
    }

    /// A new one, for `zou secrets key`.
    pub fn generate() -> Result<Key, String> {
        let mut raw = [0u8; 32];
        getrandom::fill(&mut raw).map_err(|e| format!("random key: {e}"))?;
        Ok(Key(raw))
    }

    /// How it is written down, which is how it is read back.
    pub fn encoded(&self) -> String {
        Base64::encode_string(&self.0)
    }

    /// This tenant's key, which is the only one anything is encrypted
    /// with.
    fn tenant(&self, tenant_ref: &str) -> [u8; 32] {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0).expect("hmac accepts any key length");
        mac.update(label(tenant_ref).as_bytes());
        let out = mac.finalize().into_bytes();
        let mut key = [0u8; 32];
        key.copy_from_slice(&out);
        key
    }
}

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn hex(text: &str) -> Result<Vec<u8>, String> {
    let raw: Vec<char> = text.chars().collect();
    raw.chunks(2)
        .map(|pair| {
            let byte: String = pair.iter().collect();
            u8::from_str_radix(&byte, 16).map_err(|_| "not hex".to_string())
        })
        .collect()
}

/// The object, which is a nonce and a ciphertext and nothing anybody
/// can read.
///
/// `updated` is in the clear on purpose. An operator asking how old a
/// project's environment is should not need the key, and the answer
/// tells nobody anything about what is in it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Sealed {
    version: u32,
    updated: u64,
    nonce: String,
    sealed: String,
}

impl Sealed {
    fn from_json(data: &[u8]) -> Result<Sealed, String> {
        let sealed: Sealed =
            serde_json::from_slice(data).map_err(|e| format!("sealed secrets: {e}"))?;
        if sealed.version > VERSION {
            return Err(format!(
                "sealed secrets format {} is newer than this binary supports ({VERSION}), upgrade zou",
                sealed.version
            ));
        }
        Ok(sealed)
    }

    fn to_json(&self) -> Vec<u8> {
        let mut out = serde_json::to_vec_pretty(self).expect("a sealed object serializes");
        out.push(b'\n');
        out
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn seal(key: &Key, tenant_ref: &str, values: &BTreeMap<String, String>) -> Result<Sealed, String> {
    let plain = serde_json::to_vec(values).expect("a secret map serializes");
    let mut nonce = [0u8; 12];
    getrandom::fill(&mut nonce).map_err(|e| format!("random nonce: {e}"))?;
    let aad = label(tenant_ref);
    let cipher = ChaCha20Poly1305::new(&key.tenant(tenant_ref).into());
    let sealed = cipher
        .encrypt(
            &Nonce::from(nonce),
            Payload {
                msg: &plain,
                aad: aad.as_bytes(),
            },
        )
        .map_err(|_| "sealing the secrets failed".to_string())?;
    Ok(Sealed {
        version: VERSION,
        updated: now(),
        nonce: Base64::encode_string(&nonce),
        sealed: Base64::encode_string(&sealed),
    })
}

fn open(key: &Key, tenant_ref: &str, sealed: &Sealed) -> Result<BTreeMap<String, String>, String> {
    let raw = Base64::decode_vec(&sealed.nonce).map_err(|_| "the nonce is not base64")?;
    let nonce: [u8; 12] = raw
        .try_into()
        .map_err(|_| "the nonce is the wrong length".to_string())?;
    let bytes = Base64::decode_vec(&sealed.sealed).map_err(|_| "the secrets are not base64")?;
    let aad = label(tenant_ref);
    let cipher = ChaCha20Poly1305::new(&key.tenant(tenant_ref).into());
    let plain = cipher
        .decrypt(
            &Nonce::from(nonce),
            Payload {
                msg: &bytes,
                aad: aad.as_bytes(),
            },
        )
        // Which of the three it was is not said, because the three are
        // a wrong key, a key from another fleet and somebody having
        // edited the object, and a caller that could tell them apart is
        // an oracle.
        .map_err(|_| {
            format!(
                "the secrets for {tenant_ref} do not open with this key, or they were tampered with"
            )
        })?;
    serde_json::from_slice(&plain)
        .map_err(|e| format!("the secrets for {tenant_ref} are not a map: {e}"))
}

/// Whether this project has secrets at all, which a node can ask
/// without holding any key.
pub fn present(store: &dyn CasStore, tenant_ref: &str) -> Result<bool, String> {
    let key = TenantLayout::new(tenant_ref).functions_secrets();
    Ok(store
        .get(&key)
        .map_err(|e| format!("store: {e}"))?
        .is_some())
}

/// This project's secrets, and an empty map for a project that has
/// none.
pub fn read(
    store: &dyn CasStore,
    tenant_ref: &str,
    key: &Key,
) -> Result<BTreeMap<String, String>, String> {
    match fetch(store, tenant_ref)? {
        None => Ok(BTreeMap::new()),
        Some((sealed, _)) => open(key, tenant_ref, &sealed),
    }
}

fn fetch(
    store: &dyn CasStore,
    tenant_ref: &str,
) -> Result<Option<(Sealed, zou_store::cas::Version)>, String> {
    let object = TenantLayout::new(tenant_ref).functions_secrets();
    match store.get(&object).map_err(|e| format!("store: {e}"))? {
        None => Ok(None),
        Some((data, version)) => Ok(Some((Sealed::from_json(&data)?, version))),
    }
}

/// Read, change, seal and swap, retrying when somebody else set a
/// secret while this one was being written.
///
/// The whole map every time, because the whole map is one ciphertext.
fn update<F>(
    store: &dyn CasStore,
    tenant_ref: &str,
    key: &Key,
    mut change: F,
) -> Result<Vec<String>, String>
where
    F: FnMut(&mut BTreeMap<String, String>) -> Result<Vec<String>, String>,
{
    let object = TenantLayout::new(tenant_ref).functions_secrets();
    for _ in 0..8 {
        let current = fetch(store, tenant_ref)?;
        let (mut values, version) = match &current {
            Some((sealed, version)) => (open(key, tenant_ref, sealed)?, Some(version)),
            None => (BTreeMap::new(), None),
        };
        let touched = change(&mut values)?;
        let sealed = seal(key, tenant_ref, &values)?;
        match store.put_if_match(&object, &sealed.to_json(), version) {
            Ok(_) => return Ok(touched),
            Err(CasError::Conflict { .. }) => continue,
            Err(e) => return Err(format!("store: {e}")),
        }
    }
    Err("something else kept setting secrets on this project, nothing was set".to_string())
}

/// Set these names to these values, leaving the rest alone, which is
/// what `supabase secrets set` does.
pub fn set(
    store: &dyn CasStore,
    tenant_ref: &str,
    key: &Key,
    pairs: &BTreeMap<String, String>,
) -> Result<Vec<String>, String> {
    for name in pairs.keys() {
        check_name(name)?;
    }
    update(store, tenant_ref, key, |values| {
        values.extend(pairs.iter().map(|(n, v)| (n.clone(), v.clone())));
        Ok(pairs.keys().cloned().collect())
    })
}

/// Take these names out, and say which of them were there. A name that
/// was not set is not an error: `unset` is a command somebody runs to
/// be sure, and being sure should not fail.
pub fn unset(
    store: &dyn CasStore,
    tenant_ref: &str,
    key: &Key,
    names: &[String],
) -> Result<Vec<String>, String> {
    update(store, tenant_ref, key, |values| {
        let mut gone = Vec::new();
        for name in names {
            if values.remove(name).is_some() {
                gone.push(name.clone());
            }
        }
        Ok(gone)
    })
}

/// A name a project is allowed to set for itself.
///
/// The character rule is the dotenv parser's, so a secret set on the
/// command line is one that could also have been written in the file
/// the dev loop reads, and a project does not find out at deploy time
/// that half its environment cannot be spelled.
fn check_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("a secret with no name".to_string());
    }
    if name.starts_with(RESERVED) {
        return Err(format!(
            "env name cannot start with {RESERVED}: {name}, those four are the server's own"
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        return Err(format!("{name} is not a name a dotenv file could hold"));
    }
    Ok(())
}

/// What `zou secrets list` prints beside a name.
///
/// Upstream prints a digest rather than the value, which is the right
/// answer: a person checking that they set the thing they meant to set
/// can compare a digest against one they compute themselves, and a
/// person reading over their shoulder learns nothing. This is the
/// sha256 of the value, first eight bytes, which is written down in
/// `docs/functions.md` so the comparison can be made.
pub fn digest(value: &str) -> String {
    use sha2::Digest;
    let full = sha2::Sha256::digest(value.as_bytes());
    full[..8].iter().map(|b| format!("{b:02x}")).collect()
}

pub const USAGE: &str = "usage: zou secrets <set <NAME=VALUE>... | set --env-file <path> | list | unset <NAME>... | key> [--target <store>] [--ref <tenant>] [--config <config.toml> | --no-config]";

/// `zou secrets`, which is upstream's four verbs on this store.
pub struct Args {
    pub names: Vec<String>,
    pub env_file: Option<std::path::PathBuf>,
    pub target: Option<String>,
    pub tenant: Option<String>,
    pub config: Option<std::path::PathBuf>,
    pub no_config: bool,
}

pub fn parse(argv: &[String]) -> Result<Args, String> {
    let mut args = Args {
        names: Vec::new(),
        env_file: None,
        target: None,
        tenant: None,
        config: None,
        no_config: false,
    };
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--env-file" => {
                let raw = it.next().ok_or("--env-file needs a value")?;
                args.env_file = Some(std::path::PathBuf::from(raw));
            }
            "--target" => args.target = Some(it.next().ok_or("--target needs a value")?.clone()),
            "--ref" => args.tenant = Some(it.next().ok_or("--ref needs a value")?.clone()),
            "--config" => {
                let raw = it.next().ok_or("--config needs a value")?;
                args.config = Some(std::path::PathBuf::from(raw));
            }
            "--no-config" => args.no_config = true,
            other if other.starts_with("--") => {
                return Err(format!("unexpected argument {other:?}\n{USAGE}"));
            }
            name => args.names.push(name.to_string()),
        }
    }
    Ok(args)
}

pub fn run(argv: &[String]) -> Result<(), String> {
    match argv.first().map(String::as_str) {
        Some("set") => set_command(&parse(&argv[1..])?),
        Some("list") => list_command(&parse(&argv[1..])?),
        Some("unset") => unset_command(&parse(&argv[1..])?),
        // No store and no project: this is the one verb that talks to
        // nothing, so it works before a fleet exists, which is when
        // somebody needs it.
        Some("key") => {
            say!("{}", Key::generate()?.encoded());
            eprintln!(
                "set ZOU_SECRET_KEY to this on every node that serves functions, and keep a copy: nothing sealed with it can be read without it"
            );
            Ok(())
        }
        Some(other) => Err(format!("unknown secrets command {other:?}\n{USAGE}")),
        None => Err(USAGE.to_string()),
    }
}

/// The store, the project and the key, which is what all three of the
/// other verbs need before they can do anything.
fn open_for(args: &Args) -> Result<(Box<dyn CasStore>, String, Key), String> {
    let project = crate::config::project(args.config.as_deref(), args.no_config)?;
    let (target, tenant) = crate::functions::place(
        args.target.as_deref(),
        args.tenant.as_deref(),
        project.as_ref(),
    )?;
    let key = Key::from_env()?.ok_or(
        "no secret key: set ZOU_SECRET_KEY or ZOU_SECRET_KEY_FILE, and `zou secrets key` prints a new one",
    )?;
    Ok((zou_store::open_store(&target)?, tenant, key))
}

/// `NAME=VALUE` pairs on the command line, or a dotenv file, which are
/// upstream's two ways of saying the same thing.
///
/// The file is read with the same parser the dev loop reads
/// `supabase/functions/.env` with, so the file a project has been
/// running against locally is the file it can deploy.
fn wanted(args: &Args) -> Result<BTreeMap<String, String>, String> {
    if let Some(path) = &args.env_file {
        if !args.names.is_empty() {
            return Err("either --env-file or NAME=VALUE pairs, not both".to_string());
        }
        let text =
            std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
        return zou_functions::dotenv(&text).map_err(|e| format!("{}: {e}", path.display()));
    }
    let mut out = BTreeMap::new();
    for pair in &args.names {
        let Some((name, value)) = pair.split_once('=') else {
            return Err(format!(
                "{pair:?} is not NAME=VALUE, and a secret cannot be set without a value"
            ));
        };
        out.insert(name.to_string(), value.to_string());
    }
    if out.is_empty() {
        return Err(format!("nothing to set\n{USAGE}"));
    }
    Ok(out)
}

fn set_command(args: &Args) -> Result<(), String> {
    let pairs = wanted(args)?;
    let (store, tenant, key) = open_for(args)?;
    let done = set(store.as_ref(), &tenant, &key, &pairs)?;
    say!("set {} on {tenant}", done.join(", "));
    say!("the functions deployed there read them the next time the project is attached");
    Ok(())
}

fn list_command(args: &Args) -> Result<(), String> {
    let (store, tenant, key) = open_for(args)?;
    let all = read(store.as_ref(), &tenant, &key)?;
    if all.is_empty() {
        say!("no secrets are set on {tenant}");
        return Ok(());
    }
    // Names and digests, which is what upstream's table holds. The
    // values are not printed by any verb here, because a command that
    // prints a secret is one somebody eventually runs in a shared
    // terminal.
    say!("{:<32} DIGEST", "NAME");
    for (name, value) in &all {
        say!("{name:<32} {}", digest(value));
    }
    Ok(())
}

fn unset_command(args: &Args) -> Result<(), String> {
    if args.names.is_empty() {
        return Err(format!("nothing to unset\n{USAGE}"));
    }
    let (store, tenant, key) = open_for(args)?;
    let gone = unset(store.as_ref(), &tenant, &key, &args.names)?;
    match gone.is_empty() {
        true => say!("none of those were set on {tenant}"),
        false => say!("unset {} on {tenant}", gone.join(", ")),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use zou_store::open_store;

    fn store() -> (tempfile::TempDir, Box<dyn CasStore>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = open_store(&dir.path().display().to_string()).expect("store");
        (dir, store)
    }

    fn pairs(of: &[(&str, &str)]) -> BTreeMap<String, String> {
        of.iter()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn a_key_is_read_the_way_it_is_written() {
        let key = Key::generate().expect("generate");
        let text = key.encoded();
        let same = Key::parse(&text).expect("parse");
        assert_eq!(key.0, same.0, "base64 goes round");
        let hex: String = key.0.iter().map(|b| format!("{b:02x}")).collect();
        let from_hex = Key::parse(&hex).expect("parse hex");
        assert_eq!(key.0, from_hex.0, "and hex is read too");
        assert!(Key::parse("nonsense").is_err(), "and nonsense is not");
        assert!(
            Key::parse(&Base64::encode_string(&[0u8; 16])).is_err(),
            "and neither is half a key"
        );
    }

    #[test]
    fn what_was_set_comes_back() {
        let (_dir, store) = store();
        let key = Key::generate().expect("generate");
        assert!(
            !present(store.as_ref(), "acme").expect("present"),
            "a project nobody set a secret on has no object"
        );
        assert!(
            read(store.as_ref(), "acme", &key).expect("read").is_empty(),
            "and reading it is empty rather than an error"
        );
        set(
            store.as_ref(),
            "acme",
            &key,
            &pairs(&[("STRIPE_KEY", "sk_live_1"), ("REGION", "eu")]),
        )
        .expect("set");
        let all = read(store.as_ref(), "acme", &key).expect("read");
        assert_eq!(all.get("STRIPE_KEY").map(String::as_str), Some("sk_live_1"));
        assert_eq!(all.get("REGION").map(String::as_str), Some("eu"));
        assert!(present(store.as_ref(), "acme").expect("present"));
    }

    #[test]
    fn setting_one_leaves_the_others_alone() {
        let (_dir, store) = store();
        let key = Key::generate().expect("generate");
        set(
            store.as_ref(),
            "acme",
            &key,
            &pairs(&[("A", "1"), ("B", "2")]),
        )
        .expect("set");
        set(store.as_ref(), "acme", &key, &pairs(&[("B", "3")])).expect("set again");
        let all = read(store.as_ref(), "acme", &key).expect("read");
        assert_eq!(
            all.get("A").map(String::as_str),
            Some("1"),
            "A is untouched"
        );
        assert_eq!(
            all.get("B").map(String::as_str),
            Some("3"),
            "B is the new one"
        );
        let gone = unset(
            store.as_ref(),
            "acme",
            &key,
            &["A".to_string(), "C".to_string()],
        )
        .expect("unset");
        assert_eq!(
            gone,
            vec!["A".to_string()],
            "and a name nobody set is not an error"
        );
        let all = read(store.as_ref(), "acme", &key).expect("read");
        assert!(!all.contains_key("A"));
        assert!(all.contains_key("B"));
    }

    #[test]
    fn nothing_readable_is_written_down() {
        let (dir, store) = store();
        let key = Key::generate().expect("generate");
        set(
            store.as_ref(),
            "acme",
            &key,
            &pairs(&[("STRIPE_KEY", "sk_live_hunter2")]),
        )
        .expect("set");
        let object = TenantLayout::new("acme").functions_secrets();
        let (raw, _) = store.get(&object).expect("get").expect("there");
        let text = String::from_utf8_lossy(&raw);
        assert!(!text.contains("STRIPE_KEY"), "not the name");
        assert!(!text.contains("sk_live_hunter2"), "and not the value");
        assert!(text.contains("\"updated\""), "the age is in the clear");
        // And the same bytes on the disk, since that is what somebody
        // who copied the bucket has.
        let walked = std::fs::read_to_string(dir.path().join(&object)).expect("on disk");
        assert!(!walked.contains("sk_live_hunter2"));
    }

    #[test]
    fn another_key_does_not_open_them() {
        let (_dir, store) = store();
        let key = Key::generate().expect("generate");
        set(store.as_ref(), "acme", &key, &pairs(&[("A", "1")])).expect("set");
        let other = Key::generate().expect("generate");
        let refused = read(store.as_ref(), "acme", &other).expect_err("another key");
        assert!(refused.contains("do not open"), "{refused}");
    }

    #[test]
    fn a_ciphertext_moved_between_projects_does_not_open() {
        let (_dir, store) = store();
        let key = Key::generate().expect("generate");
        set(store.as_ref(), "acme", &key, &pairs(&[("A", "1")])).expect("set");
        let from = TenantLayout::new("acme").functions_secrets();
        let (raw, _) = store.get(&from).expect("get").expect("there");
        let to = TenantLayout::new("other").functions_secrets();
        store.put(&to, &raw).expect("put");
        let refused = read(store.as_ref(), "other", &key).expect_err("another project");
        assert!(refused.contains("do not open"), "{refused}");
    }

    #[test]
    fn an_edited_object_does_not_open() {
        let (_dir, store) = store();
        let key = Key::generate().expect("generate");
        set(store.as_ref(), "acme", &key, &pairs(&[("A", "1")])).expect("set");
        let object = TenantLayout::new("acme").functions_secrets();
        let (raw, _) = store.get(&object).expect("get").expect("there");
        let mut sealed = Sealed::from_json(&raw).expect("parse");
        let mut bytes = Base64::decode_vec(&sealed.sealed).expect("base64");
        bytes[0] ^= 0xff;
        sealed.sealed = Base64::encode_string(&bytes);
        store.put(&object, &sealed.to_json()).expect("put");
        let refused = read(store.as_ref(), "acme", &key).expect_err("tampered");
        assert!(refused.contains("tampered"), "{refused}");
    }

    #[test]
    fn the_four_the_server_sets_are_refused() {
        let (_dir, store) = store();
        let key = Key::generate().expect("generate");
        let refused = set(
            store.as_ref(),
            "acme",
            &key,
            &pairs(&[("SUPABASE_URL", "http://elsewhere")]),
        )
        .expect_err("reserved");
        assert!(refused.contains("cannot start with"), "{refused}");
        let refused = set(store.as_ref(), "acme", &key, &pairs(&[("a b", "1")]))
            .expect_err("not a dotenv name");
        assert!(refused.contains("dotenv"), "{refused}");
        assert!(
            !present(store.as_ref(), "acme").expect("present"),
            "and neither of them wrote anything"
        );
    }

    /// The ciphertext is a new nonce every write, so there are no
    /// bytes here to freeze the way the other durable formats freeze
    /// theirs. What can be frozen is the envelope: the four fields
    /// around the ciphertext, which is all an older node reads before
    /// it decides whether it can open this at all.
    ///
    /// The census in crates/zou-log/tests/upgrade.rs points here, for
    /// the same reason it points at bundle.rs: zou is a binary crate.
    #[test]
    fn the_envelope_around_the_ciphertext_is_frozen() {
        let (_dir, store) = store();
        let key = Key::generate().expect("generate");
        set(
            store.as_ref(),
            "acme",
            &key,
            &pairs(&[("MY_TOKEN", "swordfish")]),
        )
        .expect("set");
        let object = TenantLayout::new("acme").functions_secrets();
        let (raw, _) = store.get(&object).expect("get").expect("there");

        let seen: serde_json::Value = serde_json::from_slice(&raw).expect("json");
        // Sorted, because the order a parsed object hands back its keys
        // depends on whether something in the build turned on serde_json's
        // preserve_order, and the format is the set of names rather than
        // the order a reader happens to see them in.
        let mut fields: Vec<&str> = seen
            .as_object()
            .expect("an object")
            .keys()
            .map(|k| k.as_str())
            .collect();
        fields.sort_unstable();
        assert_eq!(fields, ["nonce", "sealed", "updated", "version"]);
        assert_eq!(seen["version"], serde_json::json!(VERSION));
        assert!(seen["nonce"].is_string() && seen["sealed"].is_string());
        // The clear fields are the clear fields. Anything of the
        // project's own that reached them would be a leak rather than
        // a format change, and this is the one place both are visible.
        let text = String::from_utf8(raw).expect("utf8");
        assert!(
            !text.contains("MY_TOKEN") && !text.contains("swordfish"),
            "{text}"
        );
    }

    #[test]
    fn secrets_from_a_later_zou_are_refused_by_name() {
        let (_dir, store) = store();
        let key = Key::generate().expect("generate");
        set(store.as_ref(), "acme", &key, &pairs(&[("A", "1")])).expect("set");
        let object = TenantLayout::new("acme").functions_secrets();
        let (raw, _) = store.get(&object).expect("get").expect("there");
        let mut sealed = Sealed::from_json(&raw).expect("parse");
        sealed.version = VERSION + 1;
        store.put(&object, &sealed.to_json()).expect("put");
        let refused = read(store.as_ref(), "acme", &key).expect_err("later format");
        // The same phrase the rest of the durable formats refuse with.
        // See the census in crates/zou-log/tests/upgrade.rs.
        assert!(
            refused.contains("newer than") && refused.contains("upgrade"),
            "{refused}"
        );
    }

    fn argv(of: &[&str]) -> Vec<String> {
        of.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn the_arguments_come_apart() {
        let args = parse(&argv(&[
            "A=1",
            "B=two words",
            "--target",
            "./store",
            "--ref",
            "acme",
            "--no-config",
        ]))
        .expect("parsed");
        assert_eq!(
            args.names,
            vec!["A=1".to_string(), "B=two words".to_string()]
        );
        assert_eq!(args.target.as_deref(), Some("./store"));
        assert_eq!(args.tenant.as_deref(), Some("acme"));
        assert!(args.no_config);
        let pairs = wanted(&args).expect("pairs");
        assert_eq!(pairs.get("B").map(String::as_str), Some("two words"));
        assert!(parse(&argv(&["--nonsense"])).is_err(), "and noise is not");
        assert!(run(&argv(&["reveal"])).is_err(), "and it is one of four");
    }

    #[test]
    fn a_name_with_no_value_is_refused() {
        let args = parse(&argv(&["STRIPE_KEY", "--no-config"])).expect("parsed");
        let refused = wanted(&args).expect_err("no value");
        assert!(refused.contains("NAME=VALUE"), "{refused}");
        let nothing = parse(&argv(&["--no-config"])).expect("parsed");
        assert!(
            wanted(&nothing).is_err(),
            "and setting nothing is not a set"
        );
    }

    #[test]
    fn a_dotenv_file_is_read_the_way_the_dev_loop_reads_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(".env");
        std::fs::write(&path, "# a comment\nA=1\nexport B=\"two\"\n").expect("write");
        let args = parse(&argv(&["--env-file", &path.display().to_string()])).expect("parsed");
        let pairs = wanted(&args).expect("pairs");
        assert_eq!(pairs.get("A").map(String::as_str), Some("1"));
        assert_eq!(pairs.get("B").map(String::as_str), Some("two"));
        let both =
            parse(&argv(&["A=1", "--env-file", &path.display().to_string()])).expect("parsed");
        assert!(wanted(&both).is_err(), "and it is one way or the other");
    }

    #[test]
    fn a_digest_is_the_value_and_not_the_name() {
        assert_eq!(digest("sk_live_1"), digest("sk_live_1"));
        assert_ne!(digest("sk_live_1"), digest("sk_live_2"));
        assert_eq!(digest("").len(), 16, "eight bytes as hex");
    }
}
