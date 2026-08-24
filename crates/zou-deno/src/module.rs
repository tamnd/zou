//! Getting a function's modules into the isolate: its own files, and
//! the ones it imports from somewhere else.
//!
//! A function is `index.ts`, so the loader's first job is typescript:
//! v8 has never heard of a type annotation and something has to take
//! them out before it sees the file. `deno_ast` is the same swc based
//! transpiler Deno itself uses, so what runs here is what would run
//! there rather than a second interpretation of the language.
//!
//! The second job is everything a function imports that is not beside
//! it. `npm:` and `jsr:` are what the examples are written with, and by
//! default neither is resolved the way Deno resolves it: both are
//! rewritten to a url on a registry that serves packages as modules,
//! `esm.sh` by default, and from there a package is an ordinary graph
//! of `https:` imports that this loader has to handle anyway. What that
//! costs is written down in `docs/functions.md`: what runs is the
//! registry's build of the package rather than the tarball npm would
//! have unpacked.
//!
//! `ZOU_NPM=tarball` is the other answer, and it is Deno's: an `npm:`
//! specifier is the tarball, unpacked under the cache and resolved with
//! node's rules, commonjs and all. Which of the two runs more of other
//! people's code is a question for the corpus rather than for an
//! argument, so both are here and the corpus decides which is the
//! default.
//!
//! Anything fetched is kept on disk, keyed by url, so a second cold
//! start does not repeat the first one's downloads and a deployment can
//! be handed a warm cache instead of a network.

use std::cell::RefCell;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::SystemTime;

use deno_core::{
    ModuleLoadResponse, ModuleLoader, ModuleResolveResponse, ModuleSource, ModuleSourceCode,
    ModuleSpecifier, ModuleType, ResolutionKind,
};
use deno_error::JsErrorBox;

use crate::imports::Imports;

/// Where a `npm:` or `jsr:` specifier is fetched from. Overridable with
/// `ZOU_MODULE_REGISTRY`, because a project that will not reach esm.sh
/// should be able to point at its own mirror rather than give up on the
/// two specifiers every example is written with.
const REGISTRY: &str = "https://esm.sh";

/// Which of the registry's builds a package is asked for, as the query
/// that asks for it, or nothing for whatever the registry serves by
/// default. Overridable with `ZOU_MODULE_BUILD`, where an empty value
/// is the registry's default, because a mirror that is not esm.sh has
/// its own idea of what a query means.
const BUILD: Option<&str> = Some("dev");

/// The module that holds every node built in, which is the only
/// specifier this runtime invents. It is written into the module that
/// stands in for a commonjs script, because a `require` in the middle
/// of a script needs its answer to be a value already.
pub(crate) const BUILTINS: &str = "zou:node";

/// What an `npm:` specifier means to this loader.
///
/// Two answers, because they are two different runtimes to be. A
/// registry that serves packages as modules is one https graph and no
/// node in it, which is what this has always done. The tarball is what
/// npm publishes and what node runs: node module resolution, the
/// package's own `exports`, and commonjs where the package ships
/// commonjs. The second is the one Deno is, and the corpus is what says
/// when it becomes the default here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Npm {
    /// The package's build on esm.sh or a mirror of it.
    Registry,
    /// The tarball off the npm registry, unpacked and resolved.
    Tarball,
}

/// Loads a function's modules, off the disk it was deployed onto and
/// off the network for the ones that are not there.
pub struct Disk {
    registry: String,
    /// Which of the two an `npm:` specifier is, out of `ZOU_NPM`.
    npm: Npm,
    /// The query appended to a package's url, which is how the build is
    /// asked for.
    build: Option<String>,
    /// Where fetched modules live between runs.
    cache: PathBuf,
    /// Whether this server may fetch at all. A deployment that warmed
    /// its cache and wants a cold start that touches nothing sets it.
    cached_only: bool,
    /// What a bare specifier means to this function, when it has a
    /// `deno.json` or an `import_map.json` saying so.
    imports: Option<Imports>,
    /// Every file off this disk that went into the isolate, and what
    /// its clock said at the time. An isolate kept between calls is
    /// only the function that was deployed for as long as none of these
    /// has moved, which is what hot reload is.
    read: Reads,
    /// Where a specifier ended up, for the ones that ended up somewhere
    /// other than where they were asked for.
    landed: Landed,
}

/// What a redirect said, kept per isolate. A `RefCell` and not a lock
/// because a loader belongs to one isolate and an isolate belongs to
/// one thread, which is the same reason `Reads` is one.
type Landed = Rc<RefCell<std::collections::HashMap<String, String>>>;

/// The files an isolate was built out of, shared with whoever is going
/// to ask whether they have changed. An `Rc` because a module loader
/// belongs to one isolate and an isolate belongs to one thread.
pub type Reads = Rc<RefCell<Vec<(PathBuf, Option<SystemTime>)>>>;

/// Whether any file that went into an isolate has changed since it did.
///
/// A file that has been deleted counts as changed, and so does one
/// whose modification time the filesystem will not say: the answer to
/// not knowing is to build the isolate again, which costs a cold start
/// and cannot serve anybody stale code.
pub fn changed(read: &Reads) -> bool {
    read.borrow()
        .iter()
        .any(|(path, when)| &mtime(path) != when)
}

fn mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path)
        .ok()
        .and_then(|it| it.modified().ok())
}

impl Disk {
    /// The loader as the environment describes it.
    pub fn new(imports: Option<Imports>) -> Disk {
        let read = Reads::default();
        // The map's own files count as files this isolate was built
        // out of, from before it has read a module.
        if let Some(imports) = &imports {
            let mut seen = read.borrow_mut();
            for source in &imports.sources {
                let when = mtime(source);
                seen.push((source.clone(), when));
            }
        }
        Disk {
            registry: named("ZOU_MODULE_REGISTRY")
                .map(|it| it.trim_end_matches('/').to_string())
                .unwrap_or_else(|| REGISTRY.to_string()),
            build: match std::env::var("ZOU_MODULE_BUILD") {
                Ok(asked) => Some(asked).filter(|it| !it.is_empty()),
                Err(_) => BUILD.map(str::to_string),
            },
            npm: match named("ZOU_NPM").as_deref() {
                Some("tarball") => Npm::Tarball,
                _ => Npm::Registry,
            },
            cache: cache(),
            cached_only: named("ZOU_MODULE_CACHE_ONLY").is_some(),
            imports,
            read,
            landed: Landed::default(),
        }
    }

    /// A package's url with the build asked for on it.
    ///
    /// esm.sh minifies what it serves, and a minified class is a class
    /// with a one letter name. That is not cosmetic: a library that
    /// reports `this.constructor.name`, brands an error or compares a
    /// name reads differently here than it does on a runtime that
    /// unpacks the tarball, and every one of those differences looks
    /// like a runtime bug until somebody reads the bytes. The build
    /// under `dev` is the same package unminified, which is the one
    /// that carries the names the author wrote.
    ///
    /// A specifier that already asks the registry for something keeps
    /// what it asked for, because `npm:x@1?target=es2020` is a real
    /// line and the second query joins it rather than replacing it.
    fn built(&self, url: String) -> String {
        match &self.build {
            None => url,
            Some(build) => {
                let joiner = if url.contains('?') { '&' } else { '?' };
                format!("{url}{joiner}{build}")
            }
        }
    }

    /// A specifier as the url the module the registry served is at,
    /// when that is somewhere other than where it was asked for.
    ///
    /// Two things move it. A registry answers a version range with a
    /// redirect to the build it decided the range meant. And esm.sh
    /// answers a package with a two line module that re opens the same
    /// package under the version and the build target it picked,
    /// naming that in `x-esm-path`. Either way the url that was asked
    /// for is a name for a package rather than a place files sit next
    /// to each other in.
    ///
    /// Which is the whole of what this is for: `new URL('magick.wasm',
    /// import.meta.resolve('npm:@imagemagick/magick-wasm@^0'))` is a
    /// real line in a real function, and a version range is not a
    /// directory to resolve anything against.
    /// A specifier as one file inside a tree of unpacked packages
    /// resolves it, or nothing when the file doing the asking is not in
    /// that tree.
    ///
    /// A path stays a path, since the file it names is already on the
    /// disk. A bare name becomes an `npm:` specifier carrying the range
    /// the asking package declared, so that fetching whatever answers
    /// it happens in the load, where there is a thread to wait on:
    /// resolution here is synchronous and is on the isolate's own
    /// thread.
    fn inside(&self, specifier: &str, referrer: &str) -> Option<ModuleResolveResponse> {
        let at = ModuleSpecifier::parse(referrer).ok()?.to_file_path().ok()?;
        if !at.starts_with(self.cache.join("npm")) {
            return None;
        }
        let root = crate::cjs::rooted(&at);
        if specifier.starts_with('.') || specifier.starts_with('/') || specifier.starts_with('#') {
            let tree = crate::tree::Tree::at(&self.cache);
            let from = crate::tree::Reached { at, root };
            let reached = match tree.resolve(specifier, &from) {
                Ok(reached) => reached,
                Err(why) => {
                    return Some(Err(JsErrorBox::type_error(format!(
                        "{specifier} in {referrer}: {why}"
                    ))));
                }
            };
            return Some(match ModuleSpecifier::from_file_path(&reached.at) {
                Ok(it) => Ok(it),
                Err(()) => Err(JsErrorBox::type_error(format!(
                    "{} is not a path",
                    reached.at.display()
                ))),
            });
        }
        // A core module before the manifest, because node resolves a
        // bare name that is one to the built in without asking, and a
        // package does not declare a dependency on `os`. A name node has
        // and this does not still answers `node:`, so the refusal names
        // the built in rather than the manifest.
        if crate::node::core(specifier) {
            return Some(url(&format!("node:{specifier}")));
        }
        let (name, sub) = crate::tree::split(specifier);
        let want = crate::tree::declared(&root, &name)?;
        let tail = sub.trim_start_matches('.');
        Some(url(&format!("npm:{name}@{want}{tail}")))
    }

    fn moved(&self, asked: ModuleSpecifier) -> ModuleSpecifier {
        let landed = self.landed.borrow();
        match landed.get(asked.as_str()) {
            Some(landed) => ModuleSpecifier::parse(landed).unwrap_or(asked),
            None => asked,
        }
    }
}

impl Default for Disk {
    fn default() -> Disk {
        Disk::new(None)
    }
}

/// A variable that is set and is not empty, which is the only kind
/// worth acting on: an empty one is a shell that expanded nothing.
fn named(variable: &str) -> Option<String> {
    std::env::var(variable).ok().filter(|it| !it.is_empty())
}

/// Where fetched modules live between runs, which is one answer for the
/// whole process: a loader and a `Deno.readFile` of a url are looking in
/// the same place for the same bytes.
pub(crate) fn cache() -> PathBuf {
    named("ZOU_MODULE_CACHE").map_or_else(ordinary, PathBuf::from)
}

/// The ordinary cache directory, or a directory under the temporary one
/// on a machine that has no idea where its cache is, which is a cache
/// that does not survive a reboot rather than no cache at all.
fn ordinary() -> PathBuf {
    named("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| named("HOME").map(|it| PathBuf::from(it).join(".cache")))
        .unwrap_or_else(std::env::temp_dir)
        .join("zou")
        .join("modules")
}

impl ModuleLoader for Disk {
    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        _kind: ResolutionKind,
    ) -> ModuleResolveResponse {
        // The map first, because what it answers with is a specifier
        // like any other: `"zod": "npm:zod@3"` has to go through the
        // registry rewriting below, and `"util": "./lib/util.ts"`
        // arrives here already a url and falls out at the bottom.
        let mapped = self
            .imports
            .as_ref()
            .and_then(|imports| imports.resolve(specifier, referrer));
        let specifier: &str = mapped.as_deref().unwrap_or(specifier);
        if let Some(rest) = specifier.strip_prefix("npm:") {
            if self.npm == Npm::Tarball {
                // Left as it is, because working out which version of
                // the package that means is a question for the
                // registry and this call may not wait for one.
                return url(&format!("npm:{}", bare(rest)));
            }
            return url(&self.built(format!("{}/{}", self.registry, bare(rest))));
        }
        // A specifier written inside a package that was unpacked out of
        // a tarball, which is the one place a bare name is not a name
        // for this function to resolve: `lodash` inside a package means
        // whatever that package's manifest said it depends on.
        if self.npm == Npm::Tarball
            && let Some(answer) = self.inside(specifier, referrer)
        {
            return answer;
        }
        if let Some(rest) = specifier.strip_prefix("jsr:") {
            if self.npm == Npm::Tarball {
                // Left as it is for the same reason an `npm:` specifier
                // is: which version a range means is two http requests
                // away and this call may not wait for them.
                return url(&format!("jsr:{}", bare(rest)));
            }
            return url(&self.built(format!("{}/jsr/{}", self.registry, bare(rest))));
        }
        // The one specifier this runtime writes itself, in the module
        // that stands in for a commonjs script: the built ins, gathered
        // where that script's require can reach them. A function that
        // writes it by hand gets the same module, which is a map of
        // things it can already import by name.
        if specifier == BUILTINS {
            return url(BUILTINS);
        }
        if let Some(rest) = specifier.strip_prefix("node:") {
            if crate::node::source(rest).is_none() {
                return Err(JsErrorBox::type_error(format!(
                    "there is no node built in {rest} here, so node:{rest} cannot be imported"
                )));
            }
            return url(&format!("node:{rest}"));
        }
        // Plaintext means what arrives is whatever the network decided
        // it was, and what arrives is executed. https or nothing.
        if specifier.starts_with("http:") {
            return Err(JsErrorBox::type_error(format!(
                "a module is fetched over https and {specifier} is not"
            )));
        }
        if specifier.starts_with("data:") {
            return Err(JsErrorBox::type_error(format!(
                "the data: specifier in {specifier} is not supported yet"
            )));
        }
        deno_core::resolve_import(specifier, referrer)
            .map_err(|e| JsErrorBox::type_error(e.to_string()))
    }

    /// `import.meta.resolve`, which is the same resolution and then one
    /// more question: where is the module the registry actually served.
    ///
    /// Nothing else asks that, because an import of the same specifier
    /// wants what the registry hands out for it, polyfill import and
    /// all. What a function does with this answer is different: it
    /// resolves a file beside it.
    fn import_meta_resolve(
        &self,
        specifier: &str,
        referrer: &str,
    ) -> Result<ModuleSpecifier, deno_core::error::ModuleLoaderError> {
        let asked = self.resolve(specifier, referrer, ResolutionKind::DynamicImport)?;
        Ok(self.moved(asked))
    }

    fn load(
        &self,
        specifier: &ModuleSpecifier,
        _referrer: Option<&deno_core::ModuleLoadReferrer>,
        _options: deno_core::ModuleLoadOptions,
    ) -> ModuleLoadResponse {
        if declaration(specifier) {
            return ModuleLoadResponse::Sync(nothing(specifier));
        }
        if specifier.as_str() == BUILTINS {
            return ModuleLoadResponse::Sync(Ok(ModuleSource::new(
                ModuleType::JavaScript,
                ModuleSourceCode::String(crate::node::registry().into()),
                specifier,
                None,
            )));
        }
        // A node built in is javascript this binary carries, so it is
        // answered here and never fetched, cached or timed.
        if specifier.scheme() == "node" {
            return ModuleLoadResponse::Sync(builtin(specifier));
        }
        if specifier.scheme() == "npm" && self.npm == Npm::Tarball {
            let asked = specifier.clone();
            let cache = self.cache.clone();
            return ModuleLoadResponse::Async(Box::pin(async move {
                // On a blocking thread, because what this does is fetch
                // a tarball and write a package to the disk.
                tokio::task::spawn_blocking({
                    let asked = asked.clone();
                    move || package(&cache, &asked)
                })
                .await
                .map_err(|e| JsErrorBox::generic(format!("{asked} could not be unpacked: {e}")))?
            }));
        }
        // A jsr package is typescript and nothing else, so once the two
        // lookups have said which file the specifier means, it is an
        // ordinary module off a url and goes through the same cache
        // everything else does. The redirect is what makes the relative
        // imports inside it resolve against jsr.io rather than against
        // the `jsr:` specifier somebody wrote.
        if specifier.scheme() == "jsr" && self.npm == Npm::Tarball {
            let asked = specifier.clone();
            let cache = self.cache.clone();
            let cached_only = self.cached_only;
            let landed = Rc::clone(&self.landed);
            return ModuleLoadResponse::Async(Box::pin(async move {
                let fetched = tokio::task::spawn_blocking({
                    let asked = asked.clone();
                    move || {
                        let found = crate::jsr::entry(asked.path()).map_err(|why| {
                            JsErrorBox::type_error(format!("{asked} could not be resolved: {why}"))
                        })?;
                        let file = ModuleSpecifier::parse(&found)
                            .map_err(|e| JsErrorBox::type_error(format!("{found}: {e}")))?;
                        held(&cache, cached_only, &file)
                    }
                })
                .await
                .map_err(|e| JsErrorBox::generic(format!("{asked} could not be fetched: {e}")))??;
                landed
                    .borrow_mut()
                    .insert(asked.to_string(), fetched.url.clone());
                remote(&asked, fetched)
            }));
        }
        if specifier.scheme() == "file" {
            if let Ok(path) = specifier.to_file_path() {
                let when = mtime(&path);
                self.read.borrow_mut().push((path.clone(), when));
                // A file inside an unpacked package may be a script
                // rather than a module, and a script is served as the
                // module that stands in for it.
                if self.npm == Npm::Tarball && path.starts_with(self.cache.join("npm")) {
                    return ModuleLoadResponse::Sync(served(&self.cache, specifier, &path));
                }
            }
            return ModuleLoadResponse::Sync(read(specifier));
        }
        let asked = specifier.clone();
        let cache = self.cache.clone();
        let cached_only = self.cached_only;
        let landed = Rc::clone(&self.landed);
        // On a blocking thread, because the client that fetches it is
        // the blocking one, and there may be a dozen of these in the
        // air at once while a package's graph is walked.
        ModuleLoadResponse::Async(Box::pin(async move {
            let fetched = tokio::task::spawn_blocking({
                let asked = asked.clone();
                move || held(&cache, cached_only, &asked)
            })
            .await
            .map_err(|e| JsErrorBox::generic(format!("{asked} could not be fetched: {e}")))??;
            if let Some(own) = fetched.own.clone().filter(|it| it != asked.as_str()) {
                landed.borrow_mut().insert(asked.to_string(), own);
            }
            remote(&asked, fetched)
        }))
    }
}

/// The package part of an `npm:` or `jsr:` specifier, without the slash
/// that is allowed to follow the scheme.
///
/// `npm:/drizzle-orm@0.29.1/pg-core` and `npm:drizzle-orm@0.29.1/pg-core`
/// are the same specifier to Deno, and the first one is not something
/// somebody types: it is what a registry's own build of a package
/// imports itself with. Pasting it after the registry's url made
/// `https://esm.sh//drizzle-orm@0.29.1/pg-core`, which is a 400 and
/// took the whole graph down with it.
fn bare(rest: &str) -> &str {
    rest.trim_start_matches('/')
}

/// One package out of the npm registry, unpacked, as the module the
/// specifier named.
fn package(cache: &Path, asked: &ModuleSpecifier) -> Result<ModuleSource, JsErrorBox> {
    let (name, want, sub) = parts(asked.path());
    let tree = crate::tree::Tree::at(cache);
    let reached = tree
        .entry(&name, &want, &sub)
        .map_err(|why| JsErrorBox::type_error(format!("{asked}: {why}")))?;
    let at = ModuleSpecifier::from_file_path(&reached.at)
        .map_err(|()| JsErrorBox::type_error(format!("{} is not a path", reached.at.display())))?;
    inside(&tree, asked, &at, &reached)
}

/// A file inside a package that is already on the disk.
fn served(cache: &Path, asked: &ModuleSpecifier, at: &Path) -> Result<ModuleSource, JsErrorBox> {
    let tree = crate::tree::Tree::at(cache);
    let reached = crate::tree::Reached {
        at: at.to_path_buf(),
        root: crate::cjs::rooted(at),
    };
    inside(&tree, asked, asked, &reached)
}

/// One file out of a package, in whichever of the two languages it is
/// written in.
///
/// A module is served as itself. A script is served as the module that
/// stands in for it, which runs it and hands back what it assigned,
/// with the names read out of its source so that an `import { thing }`
/// of it compiles.
fn inside(
    tree: &crate::tree::Tree,
    asked: &ModuleSpecifier,
    landed: &ModuleSpecifier,
    reached: &crate::tree::Reached,
) -> Result<ModuleSource, JsErrorBox> {
    if crate::cjs::kind(&reached.at, &reached.root) == crate::cjs::Kind::Script
        && reached.at.extension().and_then(|it| it.to_str()) != Some("json")
    {
        // A name it could not read is a name that will be `undefined`
        // rather than a module that will not load, so a reading that
        // failed altogether is still worth serving the default for.
        let names = crate::cjs::names(tree, reached).unwrap_or_default();
        let text = crate::cjs::wrapper(landed.as_str(), &names);
        return Ok(ModuleSource::new_with_redirect(
            ModuleType::JavaScript,
            ModuleSourceCode::String(text.into()),
            asked,
            landed,
            None,
        ));
    }
    let text = std::fs::read_to_string(&reached.at)
        .map_err(|e| JsErrorBox::generic(format!("read {}: {e}", reached.at.display())))?;
    let media = deno_ast::MediaType::from_specifier(landed);
    source(asked, landed, text, media)
}

/// What is written after `npm:`: the package, the range asked of it,
/// and the subpath into it.
fn parts(rest: &str) -> (String, String, String) {
    let rest = rest.trim_start_matches('/');
    let mut segments = rest.split('/');
    let mut named = segments.next().unwrap_or_default().to_string();
    // A scoped package is two segments before anything else starts.
    if named.starts_with('@')
        && let Some(next) = segments.next()
    {
        named = format!("{named}/{next}");
    }
    let sub: Vec<&str> = segments.collect();
    let (name, want) = match named.rfind('@') {
        Some(cut) if cut > 0 => (named[..cut].to_string(), named[cut + 1..].to_string()),
        // No range at all, which the tree reads as any version and the
        // registry answers with whatever `latest` is.
        _ => (named, String::new()),
    };
    let sub = match sub.is_empty() {
        true => ".".to_string(),
        false => format!("./{}", sub.join("/")),
    };
    (name, want, sub)
}

fn url(text: &str) -> ModuleResolveResponse {
    ModuleSpecifier::parse(text).map_err(|e| JsErrorBox::type_error(format!("{text}: {e}")))
}

/// Whether this is a declaration file, which is a file about types and
/// so has no runtime code in it at all.
///
/// Importing one is a real line in real functions: two of the examples
/// in the Supabase repository start with `import
/// 'jsr:@supabase/functions-js/edge-runtime.d.ts'`, which is how a
/// project tells its editor what `Deno.serve` is. Deno resolves it for
/// the types and emits nothing, and nothing is what has to run here.
fn declaration(specifier: &ModuleSpecifier) -> bool {
    matches!(
        deno_ast::MediaType::from_specifier(specifier),
        deno_ast::MediaType::Dts | deno_ast::MediaType::Dmts | deno_ast::MediaType::Dcts
    )
}

/// A module with nothing in it, for the import that was about types.
///
/// Nothing is fetched for it either. The registry has no such file to
/// serve, because a declaration is not a package's build output, and
/// asking for one is the 404 that took two functions down.
fn nothing(specifier: &ModuleSpecifier) -> Result<ModuleSource, JsErrorBox> {
    Ok(ModuleSource::new(
        ModuleType::JavaScript,
        ModuleSourceCode::String(deno_core::FastString::from_static("")),
        specifier,
        None,
    ))
}

/// A node built in, out of this binary.
///
/// Resolution has already said there is one under this name, so a
/// missing source here is a bug rather than a function's mistake, and
/// it still says which name it was rather than panicking in a loader.
fn builtin(specifier: &ModuleSpecifier) -> Result<ModuleSource, JsErrorBox> {
    let name = specifier.path();
    let text = crate::node::source(name).ok_or_else(|| {
        JsErrorBox::type_error(format!(
            "there is no node built in {name} here, so node:{name} cannot be imported"
        ))
    })?;
    Ok(ModuleSource::new(
        ModuleType::JavaScript,
        ModuleSourceCode::String(deno_core::FastString::from_static(text)),
        specifier,
        None,
    ))
}

fn read(specifier: &ModuleSpecifier) -> Result<ModuleSource, JsErrorBox> {
    let path = specifier
        .to_file_path()
        .map_err(|()| JsErrorBox::type_error(format!("{specifier} is not a file")))?;
    let text = std::fs::read_to_string(&path)
        .map_err(|e| JsErrorBox::generic(format!("read {}: {e}", path.display())))?;
    let media = deno_ast::MediaType::from_specifier(specifier);
    source(specifier, specifier, text, media)
}

fn remote(asked: &ModuleSpecifier, fetched: Fetched) -> Result<ModuleSource, JsErrorBox> {
    let landed = ModuleSpecifier::parse(&fetched.url)
        .map_err(|e| JsErrorBox::type_error(format!("{}: {e}", fetched.url)))?;
    let media = kind(&fetched.content_type, &landed);
    let text = String::from_utf8(fetched.body)
        .map_err(|_| JsErrorBox::type_error(format!("{landed} is not utf8")))?;
    source(asked, &landed, text, media)
}

/// The same shape whichever place the text came from.
fn source(
    asked: &ModuleSpecifier,
    landed: &ModuleSpecifier,
    text: String,
    media: deno_ast::MediaType,
) -> Result<ModuleSource, JsErrorBox> {
    let (code, module) = match media {
        deno_ast::MediaType::JavaScript | deno_ast::MediaType::Mjs | deno_ast::MediaType::Cjs => {
            (text, ModuleType::JavaScript)
        }
        deno_ast::MediaType::Json => (text, ModuleType::Json),
        deno_ast::MediaType::TypeScript
        | deno_ast::MediaType::Mts
        | deno_ast::MediaType::Cts
        | deno_ast::MediaType::Dts
        | deno_ast::MediaType::Dmts
        | deno_ast::MediaType::Dcts
        | deno_ast::MediaType::Jsx
        | deno_ast::MediaType::Tsx => (stripped(landed, text, media)?, ModuleType::JavaScript),
        // Upstream's own sentence, which is deno_graph's, because the
        // person reading this line is a function author who will search
        // for it and find what everybody else writing Deno found. What
        // is left off is the two prefixes upstream's boot wraps it in,
        // `worker boot error: failed to create the graph:`, since those
        // name machinery this does not have.
        other => {
            return Err(JsErrorBox::type_error(format!(
                "Expected a JavaScript or TypeScript module, but identified a {other} module. Specifier: {landed}"
            )));
        }
    };
    // Where it was finally served from and not where it was asked for,
    // because every relative import inside it resolves against that.
    Ok(ModuleSource::new_with_redirect(
        module,
        ModuleSourceCode::String(code.into()),
        asked,
        landed,
        None,
    ))
}

/// What a server said it sent, falling back to what the url looks like.
fn kind(content_type: &str, landed: &ModuleSpecifier) -> deno_ast::MediaType {
    let said = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    match said.as_str() {
        "application/javascript"
        | "text/javascript"
        | "application/ecmascript"
        | "text/ecmascript" => deno_ast::MediaType::JavaScript,
        "application/typescript" | "text/typescript" | "video/mp2t" => {
            deno_ast::MediaType::TypeScript
        }
        "application/json" | "text/json" => deno_ast::MediaType::Json,
        "text/jsx" => deno_ast::MediaType::Jsx,
        "text/tsx" => deno_ast::MediaType::Tsx,
        _ => deno_ast::MediaType::from_specifier(landed),
    }
}

/// The same file with the types taken out.
fn stripped(
    specifier: &ModuleSpecifier,
    text: String,
    media: deno_ast::MediaType,
) -> Result<String, JsErrorBox> {
    let parsed = deno_ast::parse_module(deno_ast::ParseParams {
        specifier: specifier.clone(),
        text: text.into(),
        media_type: media,
        capture_tokens: false,
        scope_analysis: false,
        maybe_syntax: None,
    })
    .map_err(|e| JsErrorBox::type_error(e.to_string()))?;
    let emitted = parsed
        .transpile(
            &deno_ast::TranspileOptions {
                // Deno's own default, and what upstream's runtime was
                // measured doing: the TC39 proposal rather than
                // typescript's older `experimentalDecorators`, which
                // also lowers `accessor` fields, which v8 does not have.
                decorators: deno_ast::DecoratorsTranspileOption::Ecma,
                ..Default::default()
            },
            &deno_ast::TranspileModuleOptions::default(),
            &deno_ast::EmitOptions::default(),
        )
        .map_err(|e| JsErrorBox::type_error(e.to_string()))?;
    Ok(emitted.into_source().text)
}

/// A module as it was received, which is what the cache holds. The url
/// it was finally served from is part of it and not a detail of the
/// fetch, because the imports inside it are relative to that url.
#[cfg_attr(test, derive(Debug))]
struct Fetched {
    url: String,
    content_type: String,
    /// Where the module the registry served is, which is the url above
    /// unless the registry named another one. Only `import.meta.resolve`
    /// reads it, and what it is for is written down on `Disk::moved`.
    ///
    /// `None` is a cache entry from before this was written down, which
    /// is a thing that does not know rather than a thing that says the
    /// module is where it is.
    own: Option<String>,
    body: Vec<u8>,
}

/// A url is not a file name, so the file name is what the url hashes
/// to, under a directory named for the host so somebody looking at the
/// cache can see whose code is in it.
fn at(cache: &Path, url: &ModuleSpecifier) -> PathBuf {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(url.as_str().as_bytes());
    let mut name = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(name, "{byte:02x}");
    }
    cache.join(url.host_str().unwrap_or("elsewhere")).join(name)
}

/// A url read as bytes rather than as a module, which is what
/// `Deno.readFile` of an http url is.
///
/// The same cache and the same client the modules go through, because
/// what asks for this is a package reading a file of its own that sits
/// beside the module the registry served: a wasm blob next to the
/// javascript that instantiates it. Upstream resolves an `npm:`
/// specifier into a directory on disk and the file is simply there;
/// here a package is a url, so the file beside it is a url too.
///
/// This is not a new thing for a function to be able to reach. A
/// function has `fetch`, and this is that same reach through the cache
/// that has already been paid for.
///
/// `network` is false for the synchronous spelling, which will serve
/// what has already been fetched and will not start a download while an
/// isolate is stopped waiting for it.
pub fn bytes(url: &str, network: bool) -> Result<Vec<u8>, String> {
    let asked = ModuleSpecifier::parse(url).map_err(|e| format!("{url}: {e}"))?;
    let cache = cache();
    if let Some(fetched) = cached(&at(&cache, &asked)) {
        return Ok(fetched.body);
    }
    if !network {
        return Err(format!(
            "{asked} is not in the module cache, and a synchronous read will not fetch it: await Deno.readFile instead"
        ));
    }
    held(&cache, named("ZOU_MODULE_CACHE_ONLY").is_some(), &asked)
        .map(|fetched| fetched.body)
        .map_err(|why| why.to_string())
}

/// Cached, or fetched and then cached.
fn held(cache: &Path, cached_only: bool, asked: &ModuleSpecifier) -> Result<Fetched, JsErrorBox> {
    let path = at(cache, asked);
    if let Some(fetched) = cached(&path) {
        // An entry written before the cache wrote down where the module
        // the registry served is knows less than a fresh one would, so a
        // server that is allowed to fetch asks again rather than answer
        // with the less. One that is not keeps what it was handed.
        if fetched.own.is_some() || cached_only {
            return Ok(fetched);
        }
    }
    if cached_only {
        return Err(JsErrorBox::type_error(format!(
            "{asked} is not in the module cache and this server was told not to fetch"
        )));
    }
    let fetched = fetch(asked)?;
    // A module that could not be written down is still a module that
    // can be run, so a cache that will not take it is a slow start
    // rather than a failed one.
    let _ = keep(&path, &fetched);
    Ok(fetched)
}

/// What was written down last time, if all of it is still there.
fn cached(path: &Path) -> Option<Fetched> {
    let body = std::fs::read(path).ok()?;
    let about = std::fs::read_to_string(path.with_extension("about")).ok()?;
    let mut lines = about.lines();
    let url = lines.next()?.to_string();
    let content_type = lines.next().unwrap_or_default().to_string();
    let own = lines
        .next()
        .filter(|it| !it.is_empty())
        .map(|it| it.to_string());
    Some(Fetched {
        own,
        url,
        content_type,
        body,
    })
}

/// Both files or neither, which is why each is written under a name
/// nothing reads and then moved onto the one that is read: two isolates
/// fetching the same module at the same time is the ordinary case here
/// and not the unlucky one.
fn keep(path: &Path, fetched: &Fetched) -> std::io::Result<()> {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let directory = path
        .parent()
        .ok_or_else(|| std::io::Error::other("the cache has no directory"))?;
    std::fs::create_dir_all(directory)?;
    let once = |bytes: &[u8], onto: PathBuf| -> std::io::Result<()> {
        let ours = std::process::id();
        let count = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let partial = directory.join(format!(".{ours}.{count}.partial"));
        let mut file = std::fs::File::create(&partial)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&partial, onto)
    };
    // The body first, so a reader that finds the description finds the
    // bytes it describes.
    once(&fetched.body, path.to_path_buf())?;
    let about = format!(
        "{}\n{}\n{}\n",
        fetched.url,
        fetched.content_type,
        fetched.own.as_deref().unwrap_or_default()
    );
    once(about.as_bytes(), path.with_extension("about"))
}

/// One request for one module, as the loader or as somebody named.
fn get(
    asked: &ModuleSpecifier,
    named: Option<&str>,
) -> Result<ureq::http::Response<ureq::Body>, JsErrorBox> {
    let mut request = ureq::http::Request::get(asked.as_str());
    if let Some(who) = named {
        request = request.header(ureq::http::header::USER_AGENT, who);
    }
    let request = request
        .body(())
        .map_err(|e| JsErrorBox::type_error(format!("{asked}: {e}")))?;
    crate::fetch::agent()
        .run(request)
        .map_err(|e| JsErrorBox::type_error(format!("{asked} could not be fetched: {e}")))
}

/// One url off the network, as bytes, with no cache in the way.
///
/// The module cache is keyed by url and keeps what it fetched forever,
/// which is right for a module at a version and wrong for a document
/// that answers differently tomorrow. A packument is the second kind,
/// and a tarball is fetched once and then kept as the unpacked package
/// rather than as bytes, so neither wants the cache.
pub(crate) fn fetched(url: &str, accept: Option<&str>) -> Result<Vec<u8>, String> {
    let asked = ModuleSpecifier::parse(url).map_err(|e| format!("{url}: {e}"))?;
    let mut request = ureq::http::Request::get(asked.as_str());
    if let Some(accept) = accept {
        request = request.header(ureq::http::header::ACCEPT, accept);
    }
    let request = request.body(()).map_err(|e| format!("{url}: {e}"))?;
    let answer = crate::fetch::agent()
        .run(request)
        .map_err(|e| format!("{url} could not be fetched: {e}"))?;
    let status = answer.status();
    if !status.is_success() {
        return Err(format!(
            "{url} answered {} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or_default()
        ));
    }
    answer
        .into_body()
        .with_config()
        .limit(u64::MAX)
        .read_to_vec()
        .map_err(|e| format!("{url} stopped early: {e}"))
}

/// Who the registry is asked as, out of what the environment said.
///
/// Nobody, ordinarily, which is what gets the browser build and is the
/// build the examples corpus is measured on. `ZOU_MODULE_AGENT` names
/// somebody else so the other build can be asked for and counted
/// without a binary built to do it, and `deno` is the spelling for the
/// runtime's own agent, since writing the whole string out by hand is
/// a way to measure the wrong thing and not notice.
fn asked_as(said: Option<String>, own: &str) -> Option<String> {
    said.map(|it| match it.eq_ignore_ascii_case("deno") {
        true => own.to_string(),
        false => it,
    })
}

/// One module off the network, with the client the rest of this crate
/// calls out with.
///
/// A registry that serves packages as modules builds them, and a build
/// can fail: esm.sh answers 500 for `@vercel/og` because its browser
/// build hands esbuild a `.wasm` and esbuild has no loader for one.
/// Which build it was asked for is the user agent, so a 500 is asked
/// again as the runtime the registry builds for, and that is the whole
/// fallback: the answer to a build that could not be made is the other
/// build, not a function that will not load. What comes back imports
/// `node:`, which is why it is worth asking at all now that the built
/// ins are here, and the modules it re exports name the build in their
/// own path, so the rest of the graph arrives without asking twice.
fn fetch(asked: &ModuleSpecifier) -> Result<Fetched, JsErrorBox> {
    let as_who = asked_as(named("ZOU_MODULE_AGENT"), crate::fetch::user_agent());
    let answer = get(asked, as_who.as_deref())?;
    let answer = match answer.status().is_server_error() && as_who.is_none() {
        true => get(asked, Some(crate::fetch::user_agent()))?,
        false => answer,
    };
    let status = answer.status();
    if !status.is_success() {
        return Err(JsErrorBox::type_error(format!(
            "{asked} answered {} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or_default()
        )));
    }
    let content_type = answer
        .headers()
        .get(ureq::http::header::CONTENT_TYPE)
        .map(|value| String::from_utf8_lossy(value.as_bytes()).to_string())
        .unwrap_or_default();
    let url = {
        use ureq::ResponseExt;
        answer.get_uri().to_string()
    };
    // What esm.sh says the module it just served really is, which is a
    // path on the same host and is the only way to know it from the
    // outside: the module itself is two lines that re export it. A
    // registry that says nothing has said the module is where it is.
    let own = answer
        .headers()
        .get("x-esm-path")
        .map(|value| String::from_utf8_lossy(value.as_bytes()).to_string())
        .and_then(|path| ModuleSpecifier::parse(&url).ok()?.join(&path).ok())
        .map_or_else(|| url.clone(), |it| it.to_string());
    let own = Some(own);
    let body = answer
        .into_body()
        .with_config()
        .limit(crate::fetch::BODY_LIMIT)
        .read_to_vec()
        .map_err(|e| JsErrorBox::type_error(format!("{asked} could not be read: {e}")))?;
    Ok(Fetched {
        url,
        content_type,
        own,
        body,
    })
}

/// The loader an isolate is built with, and the list of files it will
/// fill in as it reads them.
///
/// The import map, when the function has one, is already among those
/// files before a module is loaded at all: editing a `deno.json` is
/// editing every function it resolves a name for, and hot reload has
/// to think so too.
pub fn loader(imports: Option<Imports>) -> (Rc<dyn ModuleLoader>, Reads) {
    let disk = Disk::new(imports);
    let read = Rc::clone(&disk.read);
    (Rc::new(disk), read)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(text: &str) -> ModuleSpecifier {
        ModuleSpecifier::parse(text).unwrap()
    }

    /// A loader that does not read the environment, because a test that
    /// depends on what the machine running it exports is not a test.
    fn loader() -> Disk {
        Disk {
            registry: REGISTRY.to_string(),
            npm: Npm::Registry,
            cache: PathBuf::from("/nowhere"),
            cached_only: true,
            imports: None,
            read: Reads::default(),
            landed: Landed::default(),
            build: BUILD.map(str::to_string),
        }
    }

    /// The same loader with nothing asked of the registry, which is
    /// what an operator pointing at a mirror gets.
    fn plain() -> Disk {
        Disk {
            build: None,
            ..loader()
        }
    }

    fn resolved(specifier: &str, referrer: &str) -> Result<ModuleSpecifier, String> {
        loader()
            .resolve(specifier, referrer, ResolutionKind::Import)
            .map_err(|e| e.to_string())
    }

    #[test]
    fn an_npm_specifier_becomes_a_url_on_the_registry() {
        let one = resolved("npm:@supabase/supabase-js@2", "file:///f/index.ts").unwrap();
        assert_eq!(one.as_str(), "https://esm.sh/@supabase/supabase-js@2?dev");
        let two = resolved("npm:zod@3.23.8/lib/index.js", "file:///f/index.ts").unwrap();
        assert_eq!(two.as_str(), "https://esm.sh/zod@3.23.8/lib/index.js?dev");
    }

    #[test]
    fn a_jsr_specifier_says_jsr_on_the_way() {
        let one = resolved("jsr:@std/encoding@1/hex", "file:///f/index.ts").unwrap();
        assert_eq!(one.as_str(), "https://esm.sh/jsr/@std/encoding@1/hex?dev");
    }

    /// The slash after the scheme is allowed and is not a second slash
    /// on the registry, which is a 400 there rather than the package.
    #[test]
    fn the_slash_a_registrys_own_build_writes_is_not_a_second_slash() {
        let one = resolved("npm:/drizzle-orm@0.29.1/pg-core", "file:///f/index.ts").unwrap();
        assert_eq!(
            one.as_str(),
            "https://esm.sh/drizzle-orm@0.29.1/pg-core?dev"
        );
        let two = resolved("jsr:/@std/encoding@1/hex", "file:///f/index.ts").unwrap();
        assert_eq!(two.as_str(), "https://esm.sh/jsr/@std/encoding@1/hex?dev");
    }

    /// A specifier that already asks the registry for something keeps
    /// what it asked for, and the build joins the query rather than
    /// starting a second one.
    #[test]
    fn a_specifier_that_already_has_a_query_keeps_it() {
        let one = resolved("npm:preact@10.19.3?target=es2022", "file:///f/index.ts").unwrap();
        assert_eq!(
            one.as_str(),
            "https://esm.sh/preact@10.19.3?target=es2022&dev"
        );
    }

    /// Nothing asked for is nothing appended, so a mirror that has its
    /// own idea of what a query means is served the bare url.
    #[test]
    fn a_registry_with_no_build_asked_of_it_is_left_alone() {
        let url = plain()
            .resolve(
                "npm:@supabase/supabase-js@2",
                "file:///f/index.ts",
                ResolutionKind::Import,
            )
            .unwrap();
        assert_eq!(url.as_str(), "https://esm.sh/@supabase/supabase-js@2");
    }

    /// A cdn that serves a module with no content type, which is one
    /// row of the examples corpus and a refusal on both servers. The
    /// sentence is upstream's, minus the prefixes its boot wraps
    /// everything in, because the person reading it is a function
    /// author searching for it.
    #[test]
    fn a_module_that_is_neither_javascript_nor_typescript_is_refused_the_way_upstream_refuses_it() {
        let landed = spec("https://cdn.skypack.dev/http-cache-semantics?dts");
        let refused = source(
            &landed,
            &landed,
            "whatever it was".to_string(),
            deno_ast::MediaType::Unknown,
        )
        .expect_err("not a module");
        assert_eq!(
            refused.to_string(),
            "Expected a JavaScript or TypeScript module, but identified a Unknown module. Specifier: https://cdn.skypack.dev/http-cache-semantics?dts"
        );
    }

    /// A declaration is a file about types, so importing one is a line
    /// with nothing to run in it and nothing to fetch for it.
    #[test]
    fn a_declaration_file_is_a_module_with_nothing_in_it() {
        for text in [
            "https://esm.sh/jsr/@supabase/functions-js/edge-runtime.d.ts",
            "file:///f/types.d.mts",
            "file:///f/types.d.cts",
        ] {
            assert!(declaration(&spec(text)), "{text}");
        }
        for text in ["file:///f/index.ts", "https://esm.sh/x@1/a.mjs"] {
            assert!(!declaration(&spec(text)), "{text}");
        }
    }

    #[test]
    fn what_a_fetched_module_imports_is_resolved_against_where_it_came_from() {
        let inside = resolved(
            "/@supabase/auth-js@2.1.0/es2022/auth-js.mjs",
            "https://esm.sh/@supabase/supabase-js@2",
        )
        .unwrap();
        assert_eq!(
            inside.as_str(),
            "https://esm.sh/@supabase/auth-js@2.1.0/es2022/auth-js.mjs"
        );
        let beside = resolved("./util.ts", "file:///f/index.ts").unwrap();
        assert_eq!(beside.as_str(), "file:///f/util.ts");
    }

    #[test]
    fn plaintext_and_data_are_refused_by_name() {
        for (specifier, said) in [
            ("http://example.com/a.js", "over https"),
            ("data:text/javascript,1", "not supported yet"),
        ] {
            let refused = resolved(specifier, "file:///f/index.ts").unwrap_err();
            assert!(refused.contains(said), "{specifier} said {refused}");
        }
    }

    /// A built in this runtime has resolves to itself, and one it does
    /// not have is a sentence naming it rather than a module that will
    /// fail somewhere further in.
    #[test]
    fn a_node_built_in_resolves_to_itself_and_the_rest_are_refused_by_name() {
        let held = resolved("node:path", "file:///f/index.ts").unwrap();
        assert_eq!(held.as_str(), "node:path");
        let sub = resolved("node:fs/promises", "file:///f/index.ts").unwrap();
        assert_eq!(sub.as_str(), "node:fs/promises");
        let refused = resolved("node:dgram", "file:///f/index.ts").unwrap_err();
        assert!(refused.contains("no node built in dgram"), "{refused}");
    }

    /// And the source is the one this binary carries, off no network.
    #[test]
    fn a_node_built_in_is_loaded_out_of_the_binary() {
        let loaded = builtin(&spec("node:events")).unwrap();
        let ModuleSourceCode::String(code) = loaded.code else {
            panic!("a built in is text");
        };
        assert!(code.as_str().contains("class EventEmitter"));
    }

    #[test]
    fn what_a_server_says_it_sent_beats_what_the_url_looks_like() {
        let url = spec("https://esm.sh/x@1");
        assert_eq!(
            kind("application/javascript; charset=utf-8", &url),
            deno_ast::MediaType::JavaScript
        );
        assert_eq!(
            kind("text/typescript", &url),
            deno_ast::MediaType::TypeScript
        );
        assert_eq!(kind("application/json", &url), deno_ast::MediaType::Json);
    }

    #[test]
    fn a_url_the_server_said_nothing_about_is_read_off_its_own_name() {
        assert_eq!(
            kind("", &spec("https://esm.sh/x/a.ts")),
            deno_ast::MediaType::TypeScript
        );
        assert_eq!(
            kind("application/octet-stream", &spec("https://esm.sh/x/a.mjs")),
            deno_ast::MediaType::Mjs
        );
    }

    #[test]
    fn two_urls_are_two_files_and_one_url_is_one_file() {
        let cache = PathBuf::from("/tmp/cache");
        let one = at(&cache, &spec("https://esm.sh/a@1"));
        assert_ne!(one, at(&cache, &spec("https://esm.sh/a@2")));
        assert_eq!(one, at(&cache, &spec("https://esm.sh/a@1")));
        assert!(one.starts_with("/tmp/cache/esm.sh"), "{}", one.display());
    }

    #[test]
    fn what_was_kept_is_what_comes_back() {
        let directory = tempfile::tempdir().unwrap();
        let path = at(directory.path(), &spec("https://esm.sh/a@1"));
        assert!(cached(&path).is_none());
        keep(
            &path,
            &Fetched {
                url: "https://esm.sh/a@1.2.3/es2022/a.mjs".to_string(),
                content_type: "application/javascript; charset=utf-8".to_string(),
                own: Some("https://esm.sh/a@1.2.3/es2022/a.mjs".to_string()),
                body: b"export const a = 1;".to_vec(),
            },
        )
        .unwrap();
        let back = cached(&path).unwrap();
        assert_eq!(back.url, "https://esm.sh/a@1.2.3/es2022/a.mjs");
        assert_eq!(back.content_type, "application/javascript; charset=utf-8");
        assert_eq!(back.body, b"export const a = 1;");
    }

    #[test]
    fn a_server_told_not_to_fetch_does_not_reach_for_what_it_does_not_have() {
        let directory = tempfile::tempdir().unwrap();
        let refused = held(directory.path(), true, &spec("https://esm.sh/a@1"))
            .unwrap_err()
            .to_string();
        assert!(refused.contains("not in the module cache"), "{refused}");
    }

    #[test]
    fn a_module_in_the_cache_is_not_fetched_again() {
        let directory = tempfile::tempdir().unwrap();
        let asked = spec("https://esm.sh/a@1");
        keep(
            &at(directory.path(), &asked),
            &Fetched {
                url: asked.to_string(),
                content_type: "application/javascript".to_string(),
                own: Some(asked.to_string()),
                body: b"export default 1;".to_vec(),
            },
        )
        .unwrap();
        // Told not to fetch, so an answer at all is an answer off the
        // disk.
        let held = held(directory.path(), true, &asked).unwrap();
        assert_eq!(held.body, b"export default 1;");
    }

    /// A cache written by a build that had two lines to say about a
    /// module rather than three is still a cache. What it does not say
    /// is where the module the registry served is, and a server that
    /// was handed it and told not to fetch runs on what it has.
    #[test]
    fn a_cache_from_before_there_was_a_third_line_is_still_a_cache() {
        let directory = tempfile::tempdir().unwrap();
        let asked = spec("https://esm.sh/a@1");
        let path = at(directory.path(), &asked);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"export default 1;").unwrap();
        std::fs::write(
            path.with_extension("about"),
            "https://esm.sh/a@1.2.3/es2022/a.mjs\napplication/javascript\n",
        )
        .unwrap();
        let held = held(directory.path(), true, &asked).unwrap();
        assert_eq!(held.url, "https://esm.sh/a@1.2.3/es2022/a.mjs");
        assert_eq!(held.own, None);
        assert_eq!(held.body, b"export default 1;");
    }

    /// What is written after `npm:`, taken apart. The awkward ones are
    /// a scope, which is two segments before the version, and a package
    /// with no version at all.
    #[test]
    fn a_specifier_is_a_package_a_range_and_a_subpath() {
        assert_eq!(
            parts("zod@3.23.8"),
            ("zod".into(), "3.23.8".into(), ".".into())
        );
        assert_eq!(
            parts("@supabase/supabase-js@2"),
            ("@supabase/supabase-js".into(), "2".into(), ".".into())
        );
        assert_eq!(
            parts("drizzle-orm@0.29.1/pg-core"),
            ("drizzle-orm".into(), "0.29.1".into(), "./pg-core".into())
        );
        assert_eq!(parts("/ms"), ("ms".into(), String::new(), ".".into()));
        assert_eq!(
            parts("@std/one@^1/deep/inside"),
            ("@std/one".into(), "^1".into(), "./deep/inside".into())
        );
    }

    /// A bare name inside a package is the range that package declared,
    /// carried on an `npm:` specifier so that the fetching happens in
    /// the load rather than here, where there is no thread to wait on.
    #[test]
    fn a_bare_name_inside_a_package_becomes_the_range_that_package_declared() {
        let cache = tempfile::tempdir().expect("a temporary cache");
        let root = cache.path().join("npm").join("asks").join("1.0.0");
        std::fs::create_dir_all(&root).expect("a package");
        std::fs::write(
            root.join("package.json"),
            r#"{"name": "asks", "version": "1.0.0", "dependencies": {"lodash": "^4.17.0"}}"#,
        )
        .expect("a manifest");
        std::fs::write(root.join("index.js"), "require('lodash');\n").expect("a file");
        let disk = Disk {
            npm: Npm::Tarball,
            cache: cache.path().to_path_buf(),
            ..loader()
        };
        let referrer = ModuleSpecifier::from_file_path(root.join("index.js")).expect("a url");
        let answer = disk
            .resolve("lodash", referrer.as_str(), ResolutionKind::Import)
            .expect("a specifier");
        assert_eq!(answer.as_str(), "npm:lodash@^4.17.0");
        // And a name it never declared is refused rather than guessed
        // at, which is the tree's rule and is why this asks the
        // manifest at all.
        let refused = disk.resolve("left-pad", referrer.as_str(), ResolutionKind::Import);
        assert!(refused.is_err(), "{refused:?}");
    }

    /// A bare core module name inside a package is the core module,
    /// whether or not this runtime carries one under that name. Node's
    /// rule, and the reason `postgres/src/index.js` can write `import os
    /// from 'os'` without declaring a dependency on os.
    #[test]
    fn a_bare_core_module_name_inside_a_package_is_the_built_in() {
        let cache = tempfile::tempdir().expect("a temporary cache");
        let root = cache.path().join("npm").join("asks").join("1.0.0");
        std::fs::create_dir_all(&root).expect("a package");
        std::fs::write(
            root.join("package.json"),
            r#"{"name": "asks", "version": "1.0.0"}"#,
        )
        .expect("a manifest");
        std::fs::write(root.join("index.js"), "import os from 'os';\n").expect("a file");
        let disk = Disk {
            npm: Npm::Tarball,
            cache: cache.path().to_path_buf(),
            ..loader()
        };
        let referrer = ModuleSpecifier::from_file_path(root.join("index.js")).expect("a url");
        let answer = disk
            .resolve("os", referrer.as_str(), ResolutionKind::Import)
            .expect("a specifier");
        assert_eq!(answer.as_str(), "node:os");
        // A name node has and this does not is answered the same way, so
        // that the refusal names the built in rather than a manifest
        // that was never going to mention it.
        let answer = disk
            .resolve("http", referrer.as_str(), ResolutionKind::Import)
            .expect("a specifier");
        assert_eq!(answer.as_str(), "node:http");
        // And a bare name that is not a core module is still the
        // manifest's business, which this package's manifest has nothing
        // to say about.
        let refused = disk.resolve("lodash", referrer.as_str(), ResolutionKind::Import);
        assert!(refused.is_err(), "{refused:?}");
    }

    /// A `jsr:` specifier is left alone under the tarball knob, because
    /// the file it means is two http requests away and this call has no
    /// thread to wait on either of them.
    #[test]
    fn a_jsr_specifier_under_the_tarball_knob_waits_for_the_load() {
        let disk = Disk {
            npm: Npm::Tarball,
            ..loader()
        };
        let one = disk
            .resolve(
                "jsr:@std/encoding@1/hex",
                "file:///f/index.ts",
                ResolutionKind::Import,
            )
            .expect("a specifier");
        assert_eq!(one.as_str(), "jsr:@std/encoding@1/hex");
        // And the slash a package's own imports carry after the scheme
        // comes off here, the same as it does for npm.
        let two = disk
            .resolve(
                "jsr:/@std/encoding@1",
                "file:///f/index.ts",
                ResolutionKind::Import,
            )
            .expect("a specifier");
        assert_eq!(two.as_str(), "jsr:@std/encoding@1");
    }

    /// The three things the environment can say about who to ask as.
    ///
    /// The one worth a test is `deno`, because a corpus measured with
    /// the literal string mistyped is a corpus measured on the browser
    /// build while its column header says otherwise.
    #[test]
    fn who_the_registry_is_asked_as_is_nobody_unless_the_environment_says() {
        let own = "Deno/2.1.4 (variant; zou/0.0.1)";
        assert_eq!(super::asked_as(None, own), None);
        assert_eq!(super::asked_as(Some("deno".into()), own), Some(own.into()));
        assert_eq!(super::asked_as(Some("Deno".into()), own), Some(own.into()));
        assert_eq!(
            super::asked_as(Some("a mirror's own client".into()), own),
            Some("a mirror's own client".into())
        );
    }
}
