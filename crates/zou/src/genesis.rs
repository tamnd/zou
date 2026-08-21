//! Making the database a project starts life with: initdb, the tenant
//! contract, and the genesis capture the whole fleet restores from.
//!
//! The contract is the roles, the grants and the auth, storage,
//! realtime, net, functions and cron schemas, and it used to be
//! installed lazily by whichever request first needed one of them. That
//! is correct and it is paid once, but the thing paying is a user's
//! request, it is three and a half seconds of DDL, and it is paid again
//! in every environment: a branch of a project, a restore on another
//! node, a cold lambda that attaches a store nobody has contracted yet.
//!
//! Installed here it is in the checkpoint instead. Genesis is captured
//! from a quiesced PGDATA before anything serves out of it, so the
//! schemas go up with the rest of the cluster, every later attach gets
//! them from the restore for nothing, and a branch inherits them from
//! its parent's pages.
//!
//! The cost is one postmaster start and one clean shutdown on the first
//! attach of a project, which happens once in its life next to an
//! initdb that already costs more.

use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use zou_pg::bootstrap::{self, BootstrapStats};
use zou_pg::restore;
use zou_store::CasStore;
use zou_store::layout::TenantLayout;

use crate::dev::SUPERUSER;

/// The socket name the contract postmaster listens on. It has no TCP
/// port at all, so this is a file name in a private directory rather
/// than anything anybody could connect to.
const PORT: u16 = 5432;

/// How long a postmaster on a fresh cluster gets to say hello. Generous
/// because the pages it reads on the way up come off the store, and a
/// bucket on the other side of a region is slower than a disk.
const READY_TIMEOUT: Duration = Duration::from_secs(120);

/// How long it gets to write its shutdown checkpoint and go. Every page
/// of the contract goes to the store in that checkpoint, so this is the
/// same wait for the same reason.
const STOP_TIMEOUT: Duration = Duration::from_secs(120);

/// initdb into `pgdata` through the patched storage manager, apply the
/// tenant contract, and capture the result as the project's genesis
/// checkpoint. `pgdata` and `pagecache` must not exist and must exist
/// respectively, which is what both callers already arrange.
pub fn make(
    store: &dyn CasStore,
    target: &str,
    tenant_ref: &str,
    pg_bin: &Path,
    pgdata: &Path,
    pagecache: &Path,
) -> Result<BootstrapStats, String> {
    let layout = TenantLayout::new(tenant_ref);
    // An attach killed partway through the last one leaves pages of a
    // cluster that never finished, and initdb on top of those is the
    // "relation pg_attrdef already exists" a second attach dies with.
    // Nothing under a manifest with no checkpoint is anyone's database.
    let cleared = bootstrap::clear_unfinished(store, &layout)?;
    if cleared > 0 {
        log::info!("{tenant_ref}: cleared {cleared} objects an unfinished attach left");
    }

    let at = Instant::now();
    initdb(target, tenant_ref, pg_bin, pgdata, pagecache)?;
    log::debug!("{tenant_ref}: initdb in {}", crate::boot::ms(at.elapsed()));

    let at = Instant::now();
    contract(target, tenant_ref, pg_bin, pgdata, pagecache)?;
    log::debug!(
        "{tenant_ref}: the tenant contract in {}",
        crate::boot::ms(at.elapsed())
    );

    let control =
        std::fs::read(pgdata.join("global/pg_control")).map_err(|e| format!("pg_control: {e}"))?;
    let redo = restore::control_redo(&control)?;
    bootstrap::capture_genesis(store, &layout, pgdata, redo)
}

fn initdb(
    target: &str,
    tenant_ref: &str,
    pg_bin: &Path,
    pgdata: &Path,
    pagecache: &Path,
) -> Result<(), String> {
    let out = Command::new(pg_bin.join("initdb"))
        .arg("-D")
        .arg(pgdata)
        // Named rather than left to the OS user, because the cluster
        // superuser is the role a project's own migrations run as, and
        // a Supabase project's is postgres wherever it is hosted.
        // Taking the OS user would make the owner of a database depend
        // on which account started the node.
        .args(["-U", SUPERUSER])
        .args(["--set", "io_method=sync"])
        // Pages live as store objects and a put is atomic on every
        // backend, so the torn write this guards against cannot be
        // observed. Set at initdb time so restarts, restores and
        // branches inherit it through the captured config.
        .args(["--set", "full_page_writes=off"])
        .env("ZOU_TARGET", target)
        .env("ZOU_TENANT", tenant_ref)
        .env("ZOU_PAGE_CACHE", pagecache)
        // Bootstrap has no page service to talk to, and unset means on,
        // so say off.
        .env("ZOU_PAGESERVE", "0")
        .output()
        .map_err(|e| format!("initdb: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "initdb for {tenant_ref} failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(())
}

/// The roles, the grants and the schemas, over a postmaster nobody
/// else can reach, stopped cleanly afterwards so the pages it wrote are
/// in a shutdown checkpoint before the capture reads pg_control.
fn contract(
    target: &str,
    tenant_ref: &str,
    pg_bin: &Path,
    pgdata: &Path,
    pagecache: &Path,
) -> Result<(), String> {
    over_a_private_postmaster(target, tenant_ref, pg_bin, pgdata, pagecache, |dsn| {
        on_a_connection(dsn, |client| async move {
            let out = zou_server::sql::bootstrap(&client)
                .await
                .map_err(|e| format!("the tenant contract: {e}"));
            drop(client);
            out
        })
    })
}

/// Start a postmaster on a private unix socket, hand its dsn to `with`,
/// and stop it whether that worked or not.
fn over_a_private_postmaster<T>(
    target: &str,
    tenant_ref: &str,
    pg_bin: &Path,
    pgdata: &Path,
    pagecache: &Path,
    with: impl FnOnce(&str) -> Result<T, String>,
) -> Result<T, String> {
    let sock = pgdata
        .parent()
        .ok_or("pgdata has no parent to put a socket beside")?
        .join("genesis-sock");
    std::fs::create_dir_all(&sock).map_err(|e| format!("create {}: {e}", sock.display()))?;
    std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o700))
        .map_err(|e| format!("chmod {}: {e}", sock.display()))?;

    let mut child = Command::new(pg_bin.join("postgres"))
        .arg("-D")
        .arg(pgdata)
        .args(["-p", &PORT.to_string()])
        .arg("-k")
        .arg(&sock)
        // No TCP at all. This postmaster exists to run one script and
        // stop, and a port would be a door onto a database that has no
        // password set yet.
        .args(["-c", "listen_addresses="])
        // What postgres changes reads, and what the publication the
        // contract creates is for. On from the first boot so that no
        // later restart has to change it.
        .args(["-c", "wal_level=logical"])
        .env("ZOU_TARGET", target)
        .env("ZOU_TENANT", tenant_ref)
        .env("ZOU_PAGE_CACHE", pagecache)
        // Same as initdb: there is no page service during a bootstrap.
        .env("ZOU_PAGESERVE", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn postgres for {tenant_ref}: {e}"))?;

    // Read rather than dropped, because a pipe nobody reads fills up
    // and the postmaster blocks writing to it, and because the reason a
    // bootstrap failed is in these lines.
    if let Some(stderr) = child.stderr.take() {
        let watched = tenant_ref.to_string();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                let Ok(line) = line else { break };
                log::debug!("{watched} genesis: {line}");
            }
        });
    }

    let dsn = format!(
        "host={} port={PORT} user={SUPERUSER} dbname=postgres",
        sock.display()
    );
    let out = ready(&mut child, &dsn).and_then(|()| with(&dsn));
    let stopped = stop(&mut child);
    let out = out?;
    stopped?;
    let _ = std::fs::remove_dir_all(&sock);
    Ok(out)
}

/// Connect until it answers, the postmaster dies, or the wait runs out.
/// A dial that works is the same signal the log line is and it does not
/// depend on parsing the log.
fn ready(child: &mut Child, dsn: &str) -> Result<(), String> {
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut last = String::new();
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(status)) => {
                return Err(format!(
                    "postgres exited ({status}) before it was ready: {last}"
                ));
            }
            Ok(None) => {}
            Err(e) => return Err(format!("wait: {e}")),
        }
        match dial(dsn) {
            Ok(()) => return Ok(()),
            Err(e) => last = e,
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(format!(
        "postgres was not ready within {}s: {last}",
        READY_TIMEOUT.as_secs()
    ))
}

/// A connection opened and closed again, which is how this asks
/// whether the postmaster is up.
fn dial(dsn: &str) -> Result<(), String> {
    on_a_connection(dsn, |client| async move {
        drop(client);
        Ok(())
    })
}

/// Run one thing over a connection to `dsn` on a runtime of its own.
/// The callers are `zou serve`'s attach path and `zou dev`'s startup,
/// neither of which is inside a runtime, and one connection that is
/// closed again is not worth a pool.
fn on_a_connection<T, F, Fut>(dsn: &str, with: F) -> Result<T, String>
where
    F: FnOnce(tokio_postgres::Client) -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))?;
    let dsn = dsn.to_string();
    rt.block_on(async move {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| format!("connect: {e}"))?;
        let pump = tokio::spawn(connection);
        let out = with(client).await;
        let _ = pump.await;
        out
    })
}

/// SIGINT is a fast shutdown: it disconnects, writes a shutdown
/// checkpoint and exits, which is exactly the state the capture wants
/// pg_control to be in.
fn stop(child: &mut Child) -> Result<(), String> {
    let pid = child.id() as libc::pid_t;
    unsafe { libc::kill(pid, libc::SIGINT) };
    let deadline = Instant::now() + STOP_TIMEOUT;
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => return Ok(()),
            // A postmaster asked to stop exits 0. Anything else means
            // the checkpoint this waited for may not have been written,
            // and capturing that would publish a genesis nothing can
            // recover from.
            Ok(Some(status)) => return Err(format!("postgres exited ({status}) on shutdown")),
            Ok(None) => std::thread::sleep(Duration::from_millis(20)),
            Err(e) => return Err(format!("wait: {e}")),
        }
    }
    unsafe { libc::kill(pid, libc::SIGKILL) };
    let _ = child.wait();
    Err(format!(
        "postgres would not shut down within {}s",
        STOP_TIMEOUT.as_secs()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A patched install, or nothing, in which case the test below
    /// skips and `cargo test` stays a build away from a database:
    ///
    ///   ZOU_PG_BIN=$PWD/build/pg/bin cargo test -p zou
    fn patched() -> Option<std::path::PathBuf> {
        let bin = std::env::var_os("ZOU_PG_BIN").map(std::path::PathBuf::from)?;
        bin.join("postgres").exists().then_some(bin)
    }

    /// The whole point of putting the contract in genesis: a node that
    /// has never seen this project, and runs no DDL of its own, finds
    /// the schemas already there after a restore.
    #[test]
    fn a_restore_of_genesis_has_the_schemas_without_installing_them() {
        let Some(pg_bin) = patched() else { return };
        let dir = tempfile::tempdir().expect("a directory to write into");
        let target = dir.path().join("store");
        std::fs::create_dir_all(&target).unwrap();
        let target = target.to_string_lossy().to_string();
        let store = zou_store::open_store(&target).unwrap();

        let first = dir.path().join("first");
        std::fs::create_dir_all(first.join("pagecache")).unwrap();
        make(
            &*store,
            &target,
            "local",
            &pg_bin,
            &first.join("pgdata"),
            &first.join("pagecache"),
        )
        .expect("a database made from nothing");

        let second = dir.path().join("second");
        std::fs::create_dir_all(second.join("pagecache")).unwrap();
        restore::restore(&target, "local", &second.join("pgdata")).expect("a restore of genesis");
        let found = over_a_private_postmaster(
            &target,
            "local",
            &pg_bin,
            &second.join("pgdata"),
            &second.join("pagecache"),
            |dsn| {
                on_a_connection(dsn, |client| async move {
                    let row = client
                        .query_one(
                            "select count(*) from pg_namespace where nspname in \
                             ('auth', 'storage', 'realtime', 'net', 'supabase_functions', 'cron')",
                            &[],
                        )
                        .await
                        .map_err(|e| format!("query: {e}"))?;
                    let count: i64 = row.get(0);
                    drop(client);
                    Ok(count)
                })
            },
        )
        .expect("a postmaster over the restored cluster");
        assert_eq!(found, 6, "every schema the contract installs is in genesis");
    }
}
