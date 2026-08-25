//! A database that already exists, so making one is not initdb.
//!
//! Opening a project against an empty store runs initdb through the
//! store, which writes about 3800 objects, one per page. That is 5.5 s
//! on a fast runner, 30 s on a laptop, and 150 s on a small vps, and it
//! is the entire cost of an embedded open: a postmaster over a store
//! that already has a database in it is 44 ms.
//!
//! So a fixture database is not initdb'd. One template is built per
//! machine and per postgres build, cached, and every fixture is a
//! branch of it: a manifest write, no pages copied, and the postmaster
//! start that was always going to be there.
//!
//! The template has to be branchable, which is more than initdb'd. A
//! tenant whose only capture is genesis makes a child that appears in
//! every listing and dies on its first page read, so the build does
//! not finish until a fold has packed one down, and refuses to publish
//! a template that would not serve.
//!
//! Getting that fold is the one place the two read paths differ. The
//! object path earns it by writing and checkpointing until the
//! background fold takes an interest. The layer path asks for it by
//! name once the postmaster is down, which is both faster and the
//! honest shape: a template is far too small to ever earn one.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use zou_pg::branching::ReadPath;
use zou_pg::redo::{RedoPool, RedoPoolConfig};
use zou_store::layout::TenantLayout;
use zou_store::{Manifest, open_store};

use crate::{Error, Kind, SUPERUSER, free_port, initdb, io, pg, start};

/// The tenant every fixture branches from.
pub(crate) const TEMPLATE: &str = "template";

/// Written last, and the only thing that makes a directory a template.
const READY: &str = "ready";

/// How long a build somebody else is doing gets before it is assumed to
/// have died with its lock still held.
const BUILD_TIMEOUT: Duration = Duration::from_secs(600);

/// Where templates live, unless the caller says otherwise.
///
/// `ZOU_TEMPLATE_CACHE` first, because a CI job that caches one
/// directory wants to name it, then the ordinary cache directory.
fn cache_root() -> PathBuf {
    if let Some(named) = std::env::var_os("ZOU_TEMPLATE_CACHE") {
        return PathBuf::from(named);
    }
    if let Some(xdg) = std::env::var_os("XDG_CACHE_HOME") {
        return PathBuf::from(xdg).join("zou/templates");
    }
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(".cache/zou/templates"),
        None => std::env::temp_dir().join("zou-templates"),
    }
}

/// What makes a template right for a caller rather than merely present.
///
/// The postgres build, since a different one may lay a database out
/// differently. The version of this crate, since what it captures is
/// part of the answer. And the tenant contract, because a fixture
/// skips the bootstrap on the strength of the template having run it,
/// so a template built against last week's auth schema has to be a
/// different template rather than a stale one. A miss builds a new one
/// beside the old, since a template that is subtly wrong is a corrupt
/// database somebody spends a day on.
fn identity(pg_bin: &Path) -> Result<String, Error> {
    let postgres = pg_bin.join("postgres");
    let meta = fs::metadata(&postgres).map_err(io(format!("stat {}", postgres.display())))?;
    let modified = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in format!(
        "{}|{}|{modified}|{}|{:016x}",
        postgres.display(),
        meta.len(),
        env!("CARGO_PKG_VERSION"),
        zou_server::sql::contract_version()
    )
    .bytes()
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    Ok(format!("pg-{hash:016x}"))
}

/// The template store for this postgres, built if it is not there.
///
/// The slow path is one initdb and one fold, once per machine. Everyone
/// who arrives while that is happening waits for it rather than doing
/// it again.
pub(crate) fn ensure(pg_bin: &Path) -> Result<PathBuf, Error> {
    let root = cache_root();
    let dir = root.join(identity(pg_bin)?);
    if dir.join(READY).is_file() {
        return Ok(scratch(dir.join("store")));
    }
    fs::create_dir_all(&root).map_err(io(format!("create {}", root.display())))?;

    let lock = dir.with_extension("lock");
    let deadline = Instant::now() + BUILD_TIMEOUT;
    loop {
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock)
        {
            Ok(_) => {
                let built = build(pg_bin, &dir);
                let _ = fs::remove_file(&lock);
                built?;
                return Ok(scratch(dir.join("store")));
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Somebody else is building it, or somebody else died
                // building it. Both look the same from here, and the
                // difference is how old the lock is.
                if dir.join(READY).is_file() {
                    return Ok(scratch(dir.join("store")));
                }
                if stale(&lock) {
                    let _ = fs::remove_file(&lock);
                    continue;
                }
                if Instant::now() >= deadline {
                    return Err(Error::new(
                        Kind::Io,
                        format!(
                            "waited {}s for the template at {} and it never appeared",
                            BUILD_TIMEOUT.as_secs(),
                            dir.display()
                        ),
                    ));
                }
                std::thread::sleep(Duration::from_millis(200));
            }
            Err(e) => return Err(io(format!("lock {}", lock.display()))(e)),
        }
    }
}

/// Say once that the template store is disposable, and hand the path
/// back.
///
/// Everything under it is either the template, which is rebuilt from
/// nothing when it is not there, or a fixture, which is dropped by the
/// handle that made it. Neither is worth an fsync per put, and the fsync
/// is about 14 ms of a create. The template's own build is durable: this
/// runs after the build has been published, never during it, so a
/// machine that loses power mid build loses a `building-*` directory
/// rather than a template that is half a database.
///
/// A template built by an older version gets the marker the first time
/// it is opened by this one, which is why this is not only done at build
/// time.
fn scratch(store: PathBuf) -> PathBuf {
    if !store.join(zou_store::cas::SCRATCH_MARKER).exists() {
        // Best effort. A read only cache directory is somebody else's
        // decision, and a template that fsyncs is slower rather than
        // broken.
        let _ = zou_store::cas::LocalFsStore::mark_scratch(&store);
    }
    store
}

/// A lock older than a build could plausibly be is a lock over a build
/// that died.
fn stale(lock: &Path) -> bool {
    let Ok(meta) = fs::metadata(lock) else {
        return false;
    };
    let Ok(modified) = meta.modified() else {
        return false;
    };
    match SystemTime::now().duration_since(modified) {
        Ok(age) => age > BUILD_TIMEOUT,
        Err(_) => false,
    }
}

/// initdb once, write until a fold packs a full capture down, and only
/// then call it a template.
fn build(pg_bin: &Path, dir: &Path) -> Result<(), Error> {
    if dir.join(READY).is_file() {
        return Ok(());
    }
    // Built beside the final name and moved into place, so a build that
    // dies leaves rubbish rather than a template that is half a
    // database.
    let building = dir.with_extension(format!("building-{}", std::process::id()));
    let _ = fs::remove_dir_all(&building);
    let store = building.join("store");
    let pgdata = building.join("pgdata");
    let pagecache = building.join("pagecache");
    let sock = building.join("sock");
    for made in [&store, &pagecache, &sock] {
        fs::create_dir_all(made).map_err(io(format!("create {}", made.display())))?;
    }
    let target = store.display().to_string();

    let out = raise(pg_bin, &target, &pgdata, &pagecache, &sock);
    if out.is_err() {
        let _ = fs::remove_dir_all(&building);
    }
    out?;

    fs::write(building.join(READY), b"template\n").map_err(io("write the ready marker"))?;
    // The runtime directory was a copy of what is in the store, and the
    // store is the template. Nothing else is worth keeping.
    for spent in [&pgdata, &pagecache, &sock] {
        let _ = fs::remove_dir_all(spent);
    }
    match fs::rename(&building, dir) {
        Ok(()) => Ok(()),
        Err(_) if dir.join(READY).is_file() => {
            // Somebody built the same template while this one was
            // building. Theirs is as good as this one.
            let _ = fs::remove_dir_all(&building);
            Ok(())
        }
        Err(e) => {
            let _ = fs::remove_dir_all(&building);
            Err(io(format!("publish {}", dir.display()))(e))
        }
    }
}

/// The database itself: initdb, then a postmaster over it until the
/// fold has left something a branch can read.
///
/// Which fold that is depends on the read path. The object path earns
/// one by writing, so the postmaster stays up for it. The layer path
/// asks for one by name, and asks after the postmaster is down, so the
/// memtable the shutdown flushed is in the layers the fold reads.
fn raise(
    pg_bin: &Path,
    target: &str,
    pgdata: &Path,
    pagecache: &Path,
    sock: &Path,
) -> Result<(), Error> {
    initdb(pg_bin, pgdata, target, TEMPLATE, pagecache)?;
    let port = free_port()?;
    let pg = start(
        &pg_bin.join("postgres"),
        pgdata,
        sock,
        port,
        target,
        TEMPLATE,
        pagecache,
        None,
    )?;
    let dsn = format!("host=127.0.0.1 port={port} user={SUPERUSER} dbname=postgres");
    let settled = contract(&dsn).and_then(|()| settle(&dsn, target, ReadPath::current()));
    let stopped = pg.stop();
    settled?;
    stopped?;
    if ReadPath::current() == ReadPath::Layers {
        fold(pg_bin, target)?;
    }
    Ok(())
}

/// Cut the image a branch of this template will stand on.
///
/// A shard publishes an image of its own once it has enough delta debt
/// to be worth one, and a template never will: it is an initdb and a
/// few thousand rows. So the seal asks for the fold rather than waits,
/// which is also where the cost belongs, once per template rather than
/// once per fixture cut from it.
fn fold(pg_bin: &Path, target: &str) -> Result<(), Error> {
    fold_source(pg_bin, target, TEMPLATE)
}

/// The same fold, asked for by name of whichever tenant is about to be
/// branched.
///
/// A template is the usual caller and the cheapest one, since it pays
/// this once for every fixture that will ever be cut from it. But a
/// database somebody opened themselves and then asked for a branch of
/// is in the same position, small enough that its own shard will never
/// decide an image is worth publishing, and the alternative is telling
/// the caller to go and write a few hundred megabytes first.
pub(crate) fn fold_source(pg_bin: &Path, target: &str, tenant: &str) -> Result<(), Error> {
    let store: Arc<dyn zou_store::CasStore> =
        Arc::from(open_store(target).map_err(|e| Error::new(Kind::Store, e))?);
    let layout = TenantLayout::new(tenant);
    let Some((data, _)) = store
        .get(&layout.manifest())
        .map_err(|e| Error::new(Kind::Store, e.to_string()))?
    else {
        return Err(Error::new(
            Kind::Store,
            format!("{tenant} wrote no manifest, so there is nothing to fold"),
        ));
    };
    let manifest =
        Manifest::from_json(&data).map_err(|e| Error::new(Kind::Store, e.to_string()))?;
    let checksums = zou_pg::restore::store_data_checksums(&*store, tenant)
        .map_err(|e| Error::new(Kind::Store, e))?;
    let pool = RedoPool::new(RedoPoolConfig {
        postgres: pg_bin.join("postgres"),
        scratch_root: std::env::temp_dir(),
        // One template is one small shard's worth of work, and the
        // build is already holding a machine somebody is waiting on.
        workers: 1,
        batch_timeout: FOLD_BATCH_TIMEOUT,
        batches_per_worker: FOLD_BATCHES_PER_WORKER,
        data_checksums: checksums,
    });
    zou_pg::branching::fold_for_branch(&store, tenant, &manifest, &pool, checksums)
        .map(|_| ())
        .map_err(|e| Error::new(Kind::Store, e))
}

/// The roles, the auth schema and the storage schema, put into the
/// template rather than into every database cut from it.
///
/// This is the expensive half of an open once initdb is gone: three
/// seconds of DDL on this machine, against 43 ms for the branch, the
/// restore and the postmaster together. [`zou_server::sql::bootstrap`]
/// skips the schemas when it finds them already there, so a fixture
/// runs the cheap half and inherits the rest.
fn contract(dsn: &str) -> Result<(), Error> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(io("tokio runtime"))?;
    let dsn = dsn.to_string();
    rt.block_on(async move {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(pg("connect"))?;
        let pump = tokio::spawn(connection);
        let out = zou_server::sql::bootstrap(&client)
            .await
            .map_err(pg("the tenant contract"));
        drop(client);
        let _ = pump.await;
        out
    })
}

/// The same cap the page service gives a redo batch, since a fold
/// asks for the same work in the same batches.
const FOLD_BATCH_TIMEOUT: Duration = Duration::from_secs(60);

/// Batches a redo worker serves before the pool replaces it, which
/// bounds postgres's invalid page tracking. A template's fold is far
/// short of this, so it is a backstop rather than a rotation.
const FOLD_BATCHES_PER_WORKER: u64 = 256;

/// Write and checkpoint until a branch of this would serve.
///
/// A capture of the pages as they are is what a child reads its
/// inherited pages out of, and on the object path the fold that packs
/// one down happens after a few checkpoints of writes rather than on
/// request. So this gives it something to fold and waits, and gives up
/// saying what it was waiting for rather than publishing a template
/// that would hand somebody a database that cannot read pg_authid.
///
/// The layer path packs its capture down by name once the postmaster
/// is down, see [`fold`], so all it needs here is a checkpoint for the
/// fold to cut at. It still has to see that checkpoint published,
/// see [`settle_layers`].
fn settle(dsn: &str, target: &str, path: ReadPath) -> Result<(), Error> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(io("tokio runtime"))?;
    if path == ReadPath::Layers {
        return settle_layers(&rt, dsn, target);
    }
    for round in 0..30 {
        let statements = if round == 0 {
            "create table public.zou_template_settling (pad text);
             insert into public.zou_template_settling
                 select repeat('x', 80) from generate_series(1, 2000);
             checkpoint;"
        } else {
            "insert into public.zou_template_settling
                 select repeat('x', 80) from generate_series(1, 2000);
             checkpoint;"
        };
        run(&rt, dsn, statements, "settling the template")?;
        std::thread::sleep(Duration::from_secs(1));
        if servable(target, TEMPLATE)? {
            // The table was scaffolding for the fold and no fixture
            // should inherit it.
            return run(
                &rt,
                dsn,
                "drop table public.zou_template_settling; checkpoint;",
                "clearing the scaffolding",
            );
        }
    }
    Err(Error::new(
        Kind::Store,
        "no fold packed a full capture down while the template was being built, \
         so a branch of it would not serve",
    ))
}

/// Checkpoint until the tenant publishes a capture that carries the
/// contract, on the layer path.
///
/// The fold that names a checkpoint runs on the pusher, and it can lose
/// a race with pg_control: it reads a redo that is already past the
/// point it was asked to fold at, gives up and says it will retry. The
/// retry needs another checkpoint to hang itself on, and the seal is
/// about to take the postmaster away. So this asks again rather than
/// waits, and refuses to publish rather than seal a template whose
/// newest capture predates the roles and the schemas, which is a
/// database every fixture cut from it would fail to log into.
fn settle_layers(rt: &tokio::runtime::Runtime, dsn: &str, target: &str) -> Result<(), Error> {
    let redo = checkpoint_redo(rt, dsn)?;
    let store = open_store(target).map_err(|e| Error::new(Kind::Store, e))?;
    let layout = TenantLayout::new(TEMPLATE);
    let at = Instant::now();
    loop {
        let Some((data, _)) = store
            .get(&layout.manifest())
            .map_err(|e| Error::new(Kind::Store, e.to_string()))?
        else {
            return Err(Error::new(
                Kind::Store,
                "the template wrote no manifest, so there is nothing to seal",
            ));
        };
        let manifest =
            Manifest::from_json(&data).map_err(|e| Error::new(Kind::Store, e.to_string()))?;
        if manifest.checkpoints.iter().any(|c| c.lsn.0 >= redo) {
            log::debug!(
                "template: the capture of {redo:#x} landed after {:?}",
                at.elapsed()
            );
            return Ok(());
        }
        if at.elapsed() > SETTLE_WAIT {
            let newest = manifest.checkpoints.last().map(|c| c.lsn.0).unwrap_or(0);
            return Err(Error::new(
                Kind::Store,
                format!(
                    "the checkpoint at {redo:#x} was not captured within {} seconds, the newest \
                     capture is still {newest:#x}, so a branch of this template would be the \
                     database as it stood before the tenant contract ran",
                    SETTLE_WAIT.as_secs()
                ),
            ));
        }
        std::thread::sleep(SETTLE_POLL);
        run(rt, dsn, "checkpoint;", "settling the template")?;
    }
}

/// How long the seal gives the pusher to publish the contract, which is
/// generous because a build machine under a full workspace test run is
/// the place this is slowest and a template is built once.
const SETTLE_WAIT: Duration = Duration::from_secs(120);

/// Between two checkpoints, long enough that a fold has a chance to
/// finish rather than be raced by the next one.
const SETTLE_POLL: Duration = Duration::from_millis(200);

/// Checkpoint, and say where the checkpoint replays from, as the plain
/// integer the manifest counts captures in.
fn checkpoint_redo(rt: &tokio::runtime::Runtime, dsn: &str) -> Result<u64, Error> {
    let dsn = dsn.to_string();
    rt.block_on(async move {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(pg("connect"))?;
        let pump = tokio::spawn(connection);
        let out = async {
            client
                .simple_query("checkpoint")
                .await
                .map_err(pg("settling the template"))?;
            let rows = client
                .simple_query(
                    "select pg_wal_lsn_diff(redo_lsn, '0/0')::bigint from pg_control_checkpoint()",
                )
                .await
                .map_err(pg("the checkpoint position"))?;
            rows.iter()
                .find_map(|row| match row {
                    tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0),
                    _ => None,
                })
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| Error::new(Kind::Postgres, "pg_control_checkpoint said nothing"))
        }
        .await;
        drop(client);
        let _ = pump.await;
        out
    })
}

/// One connection, one batch, and the connection closed behind it.
fn run(
    rt: &tokio::runtime::Runtime,
    dsn: &str,
    statements: &str,
    what: &'static str,
) -> Result<(), Error> {
    let dsn = dsn.to_string();
    let statements = statements.to_string();
    rt.block_on(async move {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(pg("connect"))?;
        let pump = tokio::spawn(connection);
        let out = client.batch_execute(&statements).await.map_err(pg(what));
        drop(client);
        let _ = pump.await;
        out
    })
}

/// Whether a branch of this tenant would read the pages it inherits.
pub(crate) fn servable(target: &str, tenant: &str) -> Result<bool, Error> {
    let store = open_store(target).map_err(|e| Error::new(Kind::Store, e))?;
    let layout = TenantLayout::new(tenant);
    let Some((data, _)) = store
        .get(&layout.manifest())
        .map_err(|e| Error::new(Kind::Store, e.to_string()))?
    else {
        return Ok(false);
    };
    let manifest =
        Manifest::from_json(&data).map_err(|e| Error::new(Kind::Store, e.to_string()))?;
    zou_pg::branching::why_unbranchable(&*store, &layout, &manifest, ReadPath::current())
        .map(|why| why.is_none())
        .map_err(|e| Error::new(Kind::Store, e))
}

/// Templates this process has already branched once and read back.
///
/// A published template never changes again, so whether a branch of it
/// serves is a fact about the template rather than about the branch, and
/// checking it costs one store get per checkpoint in the chain: about
/// 3.5 ms of a create. Checking it on the first cut catches the case the
/// check is there for, a template somebody else built badly or a cache
/// directory that has been half deleted, and every fixture after that in
/// the same process inherits the answer.
static PROVEN: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

/// Cut a fixture out of the template.
///
/// This is the whole create: a manifest that names the template's
/// captures, and not one page copied.
pub(crate) fn cut(target: &str, tenant: &str) -> Result<(), Error> {
    let store = open_store(target).map_err(|e| Error::new(Kind::Store, e))?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::new(Kind::Store, "the clock is before 1970"))?
        .as_secs();
    let manifest = zou_store::branch(&*store, TEMPLATE, tenant, None, now)
        .map_err(|e| Error::new(Kind::Store, e.to_string()))?;
    let proven = PROVEN
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(target);
    if proven {
        return Ok(());
    }
    zou_pg::branching::refuse_unservable(&*store, TEMPLATE, tenant, &manifest, ReadPath::current())
        .map_err(|e| Error::new(Kind::Store, e))?;
    PROVEN
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(target.to_string());
    Ok(())
}

/// Take a fixture off the store again.
///
/// The captures it read from belong to the template and stay. What goes
/// is what this database wrote and the manifest that named it.
pub(crate) fn drop_tenant(target: &str, tenant: &str) -> Result<(), Error> {
    let store = open_store(target).map_err(|e| Error::new(Kind::Store, e))?;
    zou_pg::branching::discard(&*store, tenant)
        .map(|_| ())
        .map_err(|e| Error::new(Kind::Store, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_template_is_named_after_the_postgres_it_was_built_with() {
        let dir = std::env::temp_dir().join(format!("zou-template-id-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("scratch");
        fs::write(dir.join("postgres"), b"pretend").expect("write");
        let first = identity(&dir).expect("identity");

        fs::write(dir.join("postgres"), b"pretend it is longer").expect("write");
        let second = identity(&dir).expect("identity");
        assert_ne!(first, second, "a different build is a different template");
        assert!(first.starts_with("pg-"), "{first}");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_postgres_that_is_not_there_is_said_rather_than_hashed() {
        let e = identity(Path::new("/nowhere/at/all")).expect_err("there is no postgres there");
        assert_eq!(e.kind, Kind::Io);
        assert!(e.message.contains("/nowhere/at/all"), "{}", e.message);
    }

    #[test]
    fn the_cache_root_is_the_one_the_environment_names() {
        // SAFETY: unix, and this is the only test that reads it.
        unsafe { std::env::set_var("ZOU_TEMPLATE_CACHE", "/tmp/zou-template-cache-test") };
        assert_eq!(cache_root(), PathBuf::from("/tmp/zou-template-cache-test"));
        unsafe { std::env::remove_var("ZOU_TEMPLATE_CACHE") };
    }
}
