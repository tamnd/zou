//! What a deployed function is, in the store.
//!
//! A project on a laptop is a directory: `supabase/functions/<name>/`
//! with an `index.ts` in it, whatever it imports beside it, and a
//! config file next door saying which of them are on. A node serving a
//! thousand projects has none of that. It has a prefix on an object
//! store, so a deploy is the act of turning the first into the second,
//! and this module is both halves of it: what a deploy writes and what
//! an attach reads back.
//!
//! ```text
//! tenants/<ref>/functions/DEPLOYED           the names and what they are made of
//! tenants/<ref>/functions/blobs/<sha256>     the bytes, once each
//! ```
//!
//! Files are content addressed, so a redeploy of a project where one
//! file changed writes one object, and two projects that share a
//! dependency still keep their own copy of it, because a blob lives
//! under the tenant that deployed it and nothing is shared across the
//! prefix boundary.
//!
//! Upstream bundles into an eszip and posts it to the platform api.
//! This writes the files themselves rather than an archive of them,
//! for two reasons. An archive would have to be unpacked by every node
//! that runs the project, which is the same work plus a format, and a
//! deployment whose files are addressed by hash is one a node can pull
//! incrementally: the second attach of a project that changed one
//! function fetches one file.
//!
//! What is not here is the module graph. A deploy carries the
//! function's own directory and the shared directories beside it,
//! which is what upstream's bundler mounts and what the `_shared`
//! convention exists for, and everything remote stays remote: an
//! `npm:` or a `jsr:` or an `https:` import is resolved by the node
//! that runs the function, through the module cache it already has.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zou_store::CasStore;
use zou_store::cas::CasError;
use zou_store::layout::TenantLayout;

use zou_functions::{Layout, Settings};

/// The format of the object, so a node reading a deployment written by
/// a later zou refuses it by name rather than half understanding it.
pub const VERSION: u32 = 1;

/// What is deployed for one tenant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deployment {
    pub version: u32,
    /// When the last deploy happened, unix seconds. For the operator
    /// asking how old what is running is.
    pub deployed: u64,
    pub functions: Vec<Deployed>,
}

/// One deployed function. Every path in here is relative to the
/// project's `functions` directory and written with forward slashes,
/// so a deploy from a laptop that uses backslashes is read by a node
/// that does not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deployed {
    pub name: String,
    /// What the runtime starts at, which is `<name>/index.ts` unless
    /// the project's config named another file.
    pub entrypoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub import_map: Option<String>,
    pub verify_jwt: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub static_files: Vec<String>,
    /// Every file this function is made of, and the sha256 of its
    /// bytes. The map is the deployment: a node writes exactly these
    /// and a name that is not in here is not on the disk it runs from.
    pub files: BTreeMap<String, String>,
}

impl Deployment {
    fn empty() -> Deployment {
        Deployment {
            version: VERSION,
            deployed: now(),
            functions: Vec::new(),
        }
    }

    fn from_json(data: &[u8]) -> Result<Deployment, String> {
        let deployment: Deployment =
            serde_json::from_slice(data).map_err(|e| format!("deployed functions: {e}"))?;
        if deployment.version > VERSION {
            return Err(format!(
                "deployed functions format {} is newer than this binary supports ({VERSION}), upgrade zou",
                deployment.version
            ));
        }
        Ok(deployment)
    }

    fn to_json(&self) -> Vec<u8> {
        let mut out = serde_json::to_vec_pretty(self).expect("a deployment serializes");
        out.push(b'\n');
        out
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// What is deployed for this tenant, or None for a project nobody has
/// deployed to.
pub fn fetch(store: &dyn CasStore, tenant_ref: &str) -> Result<Option<Deployment>, String> {
    let key = TenantLayout::new(tenant_ref).functions_manifest();
    match store.get(&key).map_err(|e| format!("store: {e}"))? {
        None => Ok(None),
        Some((data, _)) => Deployment::from_json(&data).map(Some),
    }
}

/// What one deploy did, for the line it prints.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Published {
    pub names: Vec<String>,
    /// How many files the deployed functions are made of.
    pub files: usize,
    /// How many of those had to be written, which is how much of a
    /// redeploy was new.
    pub written: usize,
    pub bytes: u64,
}

/// Deploy the functions of the project at `dir` into the tenant's
/// prefix.
///
/// `only` names which of them, and an empty list is all of them, which
/// is what `supabase functions deploy` with no slug does. A name the
/// project does not serve is an error rather than an empty deployment,
/// because the usual cause is a typo and the usual consequence of
/// accepting it is a caller getting a 404 an hour later.
///
/// A deploy is a merge. Deploying one function leaves the others where
/// they are, the same way upstream's is one function at a time and
/// pruning is something a project asks for separately.
pub fn publish(
    store: &dyn CasStore,
    tenant_ref: &str,
    dir: &Path,
    layout: &Layout,
    only: &[String],
) -> Result<Published, String> {
    // The same reader the dev loop serves out of, so what is deployed
    // is what was being served: a function switched off in the config
    // file is not deployed, and neither is a directory with no
    // entrypoint in it.
    let found = zou_functions::read(dir, layout)?;
    let root = dir.join("functions");
    let mut wanted = Vec::new();
    if only.is_empty() {
        wanted.extend(found);
    } else {
        for name in only {
            let Some(function) = found.iter().find(|f| &f.name == name) else {
                return Err(format!(
                    "no function named {name} is served out of {}",
                    root.display()
                ));
            };
            wanted.push(function.clone());
        }
    }
    if wanted.is_empty() {
        return Err(format!("no functions to deploy in {}", root.display()));
    }

    // The shared directories, once for the whole deploy rather than
    // once per function: `_shared` is the convention every example
    // project imports out of, and a function that does not import from
    // it is carrying a few kilobytes it does not read.
    let mut shared = Vec::new();
    for entry in std::fs::read_dir(&root).map_err(|e| format!("read {}: {e}", root.display()))? {
        let entry = entry.map_err(|e| format!("read {}: {e}", root.display()))?;
        let name = entry.file_name().to_string_lossy().to_string();
        if entry.path().is_dir() && name.starts_with('_') {
            shared.push(entry.path());
        }
    }

    // Bytes first, and the manifest last. A blob nothing names yet is
    // an object waiting to be swept; a manifest naming a blob that is
    // not there is a project that answers 500 until somebody deploys
    // again.
    let tenant = TenantLayout::new(tenant_ref);
    let mut deployed = Vec::new();
    let mut published = Published::default();
    let mut sent: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for function in &wanted {
        let mut files = BTreeMap::new();
        let mut carry = |path: &Path, files: &mut BTreeMap<String, String>| -> Result<(), String> {
            let rel = relative(&root, path)?;
            let data = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
            let sha = sha256(&data);
            files.insert(rel, sha.clone());
            if !sent.insert(sha.clone()) {
                return Ok(());
            }
            published.bytes += data.len() as u64;
            match store.put_if_absent(&tenant.functions_blob(&sha), &data) {
                Ok(_) => published.written += 1,
                // The bytes under a content addressed key are the bytes
                // being written, so somebody having got there first is
                // the same as having written it.
                Err(CasError::AlreadyExists { .. }) => {}
                Err(e) => return Err(format!("store: {e}")),
            }
            Ok(())
        };
        // The function's own directory, which for a function the config
        // file named rather than the listing is the one its entrypoint
        // is in: `functions/mcp/simple-mcp-server` and not a
        // `functions/simple-mcp-server` nobody made. Carrying only the
        // entrypoint there would deploy a function without the files
        // beside it that it imports.
        let own = root.join(&function.name);
        let own = match own.is_dir() {
            true => own,
            false => function.entrypoint.parent().unwrap_or(&root).to_path_buf(),
        };
        for path in walk(&own)? {
            carry(&path, &mut files)?;
        }
        for dir in &shared {
            for path in walk(dir)? {
                carry(&path, &mut files)?;
            }
        }
        // The entrypoint and the import map may be anywhere the config
        // file pointed, so they are carried by name as well as by the
        // walk above, which is what makes a project whose entrypoint is
        // outside its own directory deploy rather than half deploy.
        carry(&function.entrypoint, &mut files)?;
        if let Some(map) = &function.import_map {
            carry(map, &mut files)?;
        }
        for pattern in &function.static_files {
            if relative(&root, pattern).is_err() {
                log::warn!(
                    "{}: static files outside the functions directory are not deployed: {}",
                    function.name,
                    pattern.display()
                );
            }
        }
        published.files += files.len();
        published.names.push(function.name.clone());
        deployed.push(Deployed {
            name: function.name.clone(),
            entrypoint: relative(&root, &function.entrypoint)?,
            import_map: match &function.import_map {
                Some(map) => Some(relative(&root, map)?),
                None => None,
            },
            verify_jwt: function.verify_jwt,
            static_files: function
                .static_files
                .iter()
                .filter_map(|p| relative(&root, p).ok())
                .collect(),
            files,
        });
    }

    swap(store, &tenant, &deployed)?;
    Ok(published)
}

/// Merge these functions into what is deployed, retrying the read when
/// somebody deployed a different function while this one was
/// uploading. Two deploys of the same function still resolve to one of
/// them, which is the same answer two people running the command at
/// once would get from upstream.
fn swap(store: &dyn CasStore, tenant: &TenantLayout, deploying: &[Deployed]) -> Result<(), String> {
    let key = tenant.functions_manifest();
    for _ in 0..8 {
        let current = store.get(&key).map_err(|e| format!("store: {e}"))?;
        let (mut deployment, version) = match &current {
            Some((data, version)) => (Deployment::from_json(data)?, Some(version)),
            None => (Deployment::empty(), None),
        };
        deployment
            .functions
            .retain(|f| !deploying.iter().any(|d| d.name == f.name));
        deployment.functions.extend(deploying.iter().cloned());
        deployment.functions.sort_by(|a, b| a.name.cmp(&b.name));
        deployment.version = VERSION;
        deployment.deployed = now();
        match store.put_if_match(&key, &deployment.to_json(), version) {
            Ok(_) => return Ok(()),
            Err(CasError::Conflict { .. }) => continue,
            Err(e) => return Err(format!("store: {e}")),
        }
    }
    Err("something else kept deploying to this project, nothing was deployed".to_string())
}

/// Write what is deployed for this tenant into `into`, shaped like the
/// project it came from, and say what a server should read it with.
///
/// The materialized directory is a project directory: `functions/` with
/// a directory per function under it, which is the layout the listing
/// reader already understands, so a node runs deployed functions
/// through exactly the same path a laptop runs local ones through
/// rather than through a second implementation of the same rules.
///
/// None for a tenant nobody has deployed to, which is most of them.
pub fn materialize(
    store: &dyn CasStore,
    tenant_ref: &str,
    into: &Path,
) -> Result<Option<(PathBuf, Layout)>, String> {
    let Some(deployment) = fetch(store, tenant_ref)? else {
        return Ok(None);
    };
    if deployment.functions.is_empty() {
        return Ok(None);
    }
    let tenant = TenantLayout::new(tenant_ref);
    let root = into.join("functions");
    let mut layout = Layout::default();
    for function in &deployment.functions {
        for (rel, sha) in &function.files {
            let path = under(&root, rel)?;
            if path.is_file() {
                // Two functions that share a directory share its files,
                // and a file already written is a file already checked.
                continue;
            }
            let key = tenant.functions_blob(sha);
            let Some((data, _)) = store.get(&key).map_err(|e| format!("store: {e}"))? else {
                return Err(format!(
                    "{tenant_ref} deploys {rel} for {} and its bytes are not in the store",
                    function.name
                ));
            };
            // A blob is named by its own hash, so this compares the
            // store's answer against the question. It costs one pass
            // over bytes that were about to be parsed as javascript by
            // an engine that trusts them.
            if sha256(&data) != *sha {
                return Err(format!(
                    "{tenant_ref} deploys {rel} for {}, and the object under {key} is not those bytes",
                    function.name
                ));
            }
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("create {}: {e}", parent.display()))?;
            }
            std::fs::write(&path, &data).map_err(|e| format!("write {}: {e}", path.display()))?;
        }
        layout.settings.insert(
            function.name.clone(),
            Settings {
                enabled: true,
                verify_jwt: function.verify_jwt,
                // Relative to `into`, which is what the listing reader
                // resolves a config file's paths against, and `into` is
                // where this deployment's config file would have been.
                entrypoint: Some(format!("functions/{}", function.entrypoint)),
                import_map: function
                    .import_map
                    .as_ref()
                    .map(|map| format!("functions/{map}")),
                static_files: function
                    .static_files
                    .iter()
                    .map(|p| format!("functions/{p}"))
                    .collect(),
            },
        );
    }
    Ok(Some((into.to_path_buf(), layout)))
}

/// Every file under `dir`, in the order the paths sort, or nothing at
/// all when there is no such directory.
///
/// Dotfiles go: a `.env` beside a function is the one thing in a
/// project that must never leave it, because a deployment's secrets
/// come from the tenant rather than from whatever was on the laptop
/// that ran the deploy. `.git` and the rest go with it, on the same
/// argument the listing reader skips them: nothing under a name
/// starting with a dot is code a function imports.
fn walk(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(format!("read {}: {e}", dir.display())),
        };
        for entry in entries {
            let entry = entry.map_err(|e| format!("read {}: {e}", dir.display()))?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with('.') {
                continue;
            }
            let path = entry.path();
            // Read rather than followed, the same as everywhere else a
            // project's files are handled: a link is deployed as the
            // bytes it points at when it points inside the project, and
            // a link out of the project is a file that cannot be read
            // and says so.
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

/// `path` as the deployment writes it: relative to the functions
/// directory, with forward slashes.
fn relative(root: &Path, path: &Path) -> Result<String, String> {
    let rel = path
        .strip_prefix(root)
        .map_err(|_| format!("{} is not under {}", path.display(), root.display()))?;
    let mut out = String::new();
    for part in rel.components() {
        let std::path::Component::Normal(part) = part else {
            return Err(format!(
                "{} is not a path a deployment carries",
                path.display()
            ));
        };
        if !out.is_empty() {
            out.push('/');
        }
        out.push_str(&part.to_string_lossy());
    }
    Ok(out)
}

/// The other direction, and the place a deployment written somewhere
/// else is not trusted: a path in the manifest becomes a path under
/// `root` or it is refused, so a name with a `..` in it cannot write
/// outside the directory this deployment is being unpacked into.
fn under(root: &Path, rel: &str) -> Result<PathBuf, String> {
    let mut path = root.to_path_buf();
    for part in rel.split('/') {
        if part.is_empty() || part == "." || part == ".." || part.contains('\\') {
            return Err(format!("a deployment names {rel:?}, which is not a path"));
        }
        path.push(part);
    }
    Ok(path)
}

fn sha256(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    let mut hex = String::with_capacity(digest.len() * 2);
    for b in digest {
        hex.push_str(&format!("{b:02x}"));
    }
    hex
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use zou_store::open_store;

    use super::*;

    /// A store on disk, which is the backend embedded mode and the
    /// tests share, and a project beside it.
    fn store() -> (tempfile::TempDir, Arc<dyn CasStore>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().display().to_string();
        let store: Arc<dyn CasStore> = Arc::from(open_store(&target).expect("store"));
        (dir, store)
    }

    /// A project with two functions, one of which imports out of
    /// `_shared`, which is the shape of every example project.
    fn project() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("functions");
        std::fs::create_dir_all(root.join("hello")).expect("mkdir");
        std::fs::create_dir_all(root.join("world")).expect("mkdir");
        std::fs::create_dir_all(root.join("_shared")).expect("mkdir");
        std::fs::write(
            root.join("hello/index.ts"),
            "import { who } from '../_shared/who.ts'\nDeno.serve(() => new Response(who))",
        )
        .expect("write");
        std::fs::write(
            root.join("world/index.ts"),
            "Deno.serve(() => new Response())",
        )
        .expect("write");
        std::fs::write(root.join("_shared/who.ts"), "export const who = 'nobody'").expect("write");
        std::fs::write(root.join(".env"), "SECRET=hunter2").expect("write");
        dir
    }

    #[test]
    fn a_deploy_carries_the_function_and_what_is_beside_it() {
        let (_target, store) = store();
        let project = project();
        let published = publish(
            store.as_ref(),
            "acme",
            project.path(),
            &Layout::default(),
            &[],
        )
        .expect("publish");
        assert_eq!(published.names, ["hello", "world"]);

        let deployment = fetch(store.as_ref(), "acme").expect("fetch").expect("some");
        let hello = &deployment.functions[0];
        assert_eq!(hello.entrypoint, "hello/index.ts");
        assert!(hello.verify_jwt, "upstream's default survives a deploy");
        let carried: Vec<&str> = hello.files.keys().map(String::as_str).collect();
        assert_eq!(carried, ["_shared/who.ts", "hello/index.ts"]);
        assert!(
            !hello.files.contains_key(".env"),
            "a project's secrets are the one thing that never leaves it"
        );
    }

    /// A function the config file named rather than the listing, whose
    /// directory is somewhere else entirely. What it carries is that
    /// directory, because the alternative is deploying an entrypoint
    /// without the file next to it that it imports.
    #[test]
    fn a_function_whose_directory_is_elsewhere_carries_that_directory() {
        let (_target, store) = store();
        let project = project();
        let root = project.path().join("functions");
        std::fs::create_dir_all(root.join("mcp/deeper")).expect("mkdir");
        std::fs::write(
            root.join("mcp/deeper/index.ts"),
            "import { how } from './how.ts'\nDeno.serve(() => new Response(how))",
        )
        .expect("write");
        std::fs::write(root.join("mcp/deeper/how.ts"), "export const how = 'so'").expect("write");
        let mut layout = Layout::default();
        layout.settings.insert(
            "deeper".to_string(),
            Settings {
                entrypoint: Some("./functions/mcp/deeper/index.ts".to_string()),
                ..Settings::default()
            },
        );
        let published =
            publish(store.as_ref(), "acme", project.path(), &layout, &[]).expect("publish");
        assert_eq!(published.names, ["deeper", "hello", "world"]);

        let deployment = fetch(store.as_ref(), "acme").expect("fetch").expect("some");
        let deeper = deployment
            .functions
            .iter()
            .find(|f| f.name == "deeper")
            .expect("deployed");
        assert_eq!(deeper.entrypoint, "mcp/deeper/index.ts");
        let carried: Vec<&str> = deeper.files.keys().map(String::as_str).collect();
        assert_eq!(
            carried,
            ["_shared/who.ts", "mcp/deeper/how.ts", "mcp/deeper/index.ts"]
        );

        // And it comes back the same way, through the reader a laptop
        // uses, which is the only thing that makes the round trip worth
        // asserting.
        let into = tempfile::tempdir().expect("tempdir");
        let (dir, layout) = materialize(store.as_ref(), "acme", into.path())
            .expect("materialize")
            .expect("some");
        let found = zou_functions::read(&dir, &layout).expect("read");
        let deeper = found.iter().find(|f| f.name == "deeper").expect("served");
        assert_eq!(deeper.entrypoint, dir.join("functions/mcp/deeper/index.ts"));
    }

    #[test]
    fn what_is_deployed_is_what_the_config_file_says_is_served() {
        let (_target, store) = store();
        let project = project();
        let mut layout = Layout::default();
        layout.settings.insert(
            "world".to_string(),
            Settings {
                enabled: false,
                ..Settings::default()
            },
        );
        layout.settings.insert(
            "hello".to_string(),
            Settings {
                verify_jwt: false,
                ..Settings::default()
            },
        );
        publish(store.as_ref(), "acme", project.path(), &layout, &[]).expect("publish");
        let deployment = fetch(store.as_ref(), "acme").expect("fetch").expect("some");
        let names: Vec<&str> = deployment
            .functions
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(names, ["hello"], "a function switched off is not deployed");
        assert!(!deployment.functions[0].verify_jwt);
    }

    #[test]
    fn deploying_one_function_leaves_the_others_where_they_are() {
        let (_target, store) = store();
        let project = project();
        publish(
            store.as_ref(),
            "acme",
            project.path(),
            &Layout::default(),
            &["hello".to_string()],
        )
        .expect("publish");
        publish(
            store.as_ref(),
            "acme",
            project.path(),
            &Layout::default(),
            &["world".to_string()],
        )
        .expect("publish");
        let deployment = fetch(store.as_ref(), "acme").expect("fetch").expect("some");
        let names: Vec<&str> = deployment
            .functions
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(names, ["hello", "world"]);
    }

    #[test]
    fn a_name_the_project_does_not_serve_is_refused() {
        let (_target, store) = store();
        let project = project();
        let e = publish(
            store.as_ref(),
            "acme",
            project.path(),
            &Layout::default(),
            &["nope".to_string()],
        )
        .expect_err("refused");
        assert!(e.contains("no function named nope"), "{e}");
        assert!(
            fetch(store.as_ref(), "acme").expect("fetch").is_none(),
            "and nothing was deployed"
        );
    }

    #[test]
    fn a_redeploy_only_writes_what_changed() {
        let (_target, store) = store();
        let project = project();
        let first = publish(
            store.as_ref(),
            "acme",
            project.path(),
            &Layout::default(),
            &[],
        )
        .expect("publish");
        assert_eq!(first.written, 3, "hello, world and the shared file");
        std::fs::write(
            project.path().join("functions/world/index.ts"),
            "Deno.serve(() => new Response('again'))",
        )
        .expect("write");
        let again = publish(
            store.as_ref(),
            "acme",
            project.path(),
            &Layout::default(),
            &[],
        )
        .expect("publish");
        assert_eq!(again.written, 1, "the one file that changed");
    }

    #[test]
    fn what_is_deployed_comes_back_as_a_project() {
        let (_target, store) = store();
        let project = project();
        publish(
            store.as_ref(),
            "acme",
            project.path(),
            &Layout::default(),
            &[],
        )
        .expect("publish");

        let into = tempfile::tempdir().expect("tempdir");
        let (dir, layout) = materialize(store.as_ref(), "acme", into.path())
            .expect("materialize")
            .expect("something is deployed");
        assert_eq!(dir, into.path());

        // The listing reader is the thing that has to understand what
        // came out, because it is what a server serves out of.
        let found = zou_functions::read(&dir, &layout).expect("read");
        let names: Vec<&str> = found.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["hello", "world"]);
        assert_eq!(
            found[0].entrypoint,
            into.path().join("functions/hello/index.ts")
        );
        assert_eq!(
            std::fs::read_to_string(into.path().join("functions/_shared/who.ts")).expect("read"),
            "export const who = 'nobody'",
            "what a function imports arrives with it"
        );
    }

    #[test]
    fn a_project_nobody_deployed_to_has_nothing_to_materialize() {
        let (_target, store) = store();
        let into = tempfile::tempdir().expect("tempdir");
        assert!(
            materialize(store.as_ref(), "acme", into.path())
                .expect("materialize")
                .is_none()
        );
    }

    #[test]
    fn a_deployment_that_names_a_path_out_of_the_directory_is_refused() {
        let (_target, store) = store();
        let deployed = Deployed {
            name: "hello".to_string(),
            entrypoint: "hello/index.ts".to_string(),
            import_map: None,
            verify_jwt: true,
            static_files: Vec::new(),
            files: BTreeMap::from([("../../etc/passwd".to_string(), sha256(b"x"))]),
        };
        let deployment = Deployment {
            version: VERSION,
            deployed: 0,
            functions: vec![deployed],
        };
        store
            .put(
                &TenantLayout::new("acme").functions_manifest(),
                &deployment.to_json(),
            )
            .expect("put");
        let into = tempfile::tempdir().expect("tempdir");
        let e = materialize(store.as_ref(), "acme", into.path()).expect_err("refused");
        assert!(e.contains("which is not a path"), "{e}");
    }

    #[test]
    fn a_blob_that_is_not_what_it_is_named_is_refused() {
        let (_target, store) = store();
        let project = project();
        publish(
            store.as_ref(),
            "acme",
            project.path(),
            &Layout::default(),
            &[],
        )
        .expect("publish");
        let deployment = fetch(store.as_ref(), "acme").expect("fetch").expect("some");
        let sha = deployment.functions[0]
            .files
            .get("hello/index.ts")
            .expect("carried");
        store
            .put(
                &TenantLayout::new("acme").functions_blob(sha),
                b"Deno.serve(() => new Response('somebody else'))",
            )
            .expect("put");
        let into = tempfile::tempdir().expect("tempdir");
        let e = materialize(store.as_ref(), "acme", into.path()).expect_err("refused");
        assert!(e.contains("is not those bytes"), "{e}");
    }

    /// What a deploy writes is read back by whatever node picks the
    /// project up, which during a rollout is a node on the release
    /// before this one. A field renamed, a field that stopped being
    /// optional, a bool that became a string: each of those is a
    /// project those nodes can no longer attach, and none of them
    /// moves the version number on its own. So the shape is written
    /// out here rather than round tripped, and a change to it is a
    /// diff somebody has to look at.
    ///
    /// The census in crates/zou-log/tests/upgrade.rs points here. It
    /// cannot hold this one itself: zou is a binary crate and no other
    /// crate's test can build a Deployment.
    #[test]
    fn what_a_deploy_writes_is_frozen() {
        let deployment = Deployment {
            version: VERSION,
            deployed: 1_767_100_000,
            functions: vec![Deployed {
                name: "hello".to_string(),
                entrypoint: "hello/index.ts".to_string(),
                import_map: None,
                verify_jwt: true,
                static_files: Vec::new(),
                files: BTreeMap::from([(
                    "hello/index.ts".to_string(),
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
                )]),
            }],
        };
        let frozen = r#"{
  "version": 1,
  "deployed": 1767100000,
  "functions": [
    {
      "name": "hello",
      "entrypoint": "hello/index.ts",
      "verify_jwt": true,
      "files": {
        "hello/index.ts": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
      }
    }
  ]
}
"#;
        assert_eq!(String::from_utf8(deployment.to_json()).unwrap(), frozen);
        // And it reads back as what it was, not as something plausible
        // that parsed: every field here defaults or is optional, so
        // parsing alone proves nothing.
        let back = Deployment::from_json(frozen.as_bytes()).expect("a deployment");
        assert_eq!(back, deployment);
    }

    #[test]
    fn a_deployment_from_a_later_zou_is_refused_by_name() {
        let (_target, store) = store();
        store
            .put(
                &TenantLayout::new("acme").functions_manifest(),
                br#"{"version":99,"deployed":0,"functions":[]}"#,
            )
            .expect("put");
        let e = fetch(store.as_ref(), "acme").expect_err("refused");
        // Worded the way every other durable format words it, because
        // an operator reading a log learns one phrase and this object
        // is no different from the ten in the store beside it. The
        // census in crates/zou-log/tests/upgrade.rs holds the whole
        // set, this one included.
        assert!(e.contains("newer than") && e.contains("upgrade"), "{e}");
    }
}
