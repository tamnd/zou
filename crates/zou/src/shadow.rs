//! A throwaway postgres, started and thrown away inside one command.
//!
//! `zou db diff` needs somewhere to replay the migrations so it has
//! something to compare the real database against. That somewhere has
//! to be empty, has to be gone afterwards, and has to look enough like
//! a zou database that a migration written for one applies to it: a
//! project's migration that references `auth.users` or calls
//! `auth.uid()` in a policy is ordinary, and against a bare postgres it
//! would fail for a reason that has nothing to do with the project.
//!
//! So it is a real postmaster, from the same binaries `zou dev` uses,
//! on a unix socket in a temporary directory, with the server's own
//! bootstrap run against it. It listens on no tcp port at all, which is
//! both one less thing to collide with and one less thing to reach.
//!
//! Durability is off across the board. Nothing here outlives the
//! command, and the whole directory is removed on the way out, whether
//! the diff worked or not.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tokio_postgres::{Client, NoTls};

/// The superuser the shadow is initdb'd with, which is the one a
/// project's migrations run as everywhere else.
const SUPERUSER: &str = "postgres";

/// How long to wait for the postmaster to answer before giving up. A
/// cold initdb'd cluster on a slow disk takes a couple of seconds; a
/// postmaster that is never going to answer takes forever, and this is
/// where that ends.
const READY: Duration = Duration::from_secs(30);

/// A postgres that exists for the length of one command.
pub struct Shadow {
    dir: PathBuf,
    postmaster: Option<Child>,
    /// The dsn to reach it on, a directory rather than a host because
    /// it is only listening on the socket in there.
    pub url: String,
}

/// Where the postgres binaries are, asked the same way `zou dev` asks.
pub fn pg_bin(explicit: Option<&Path>) -> PathBuf {
    explicit
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("ZOU_PG_BIN").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("build/pg/bin"))
}

impl Shadow {
    /// initdb a cluster and start a postmaster on it. Returns as soon
    /// as the directory is laid out and the child is running; nothing
    /// has connected yet, which is what `connect` is for.
    pub fn start(pg_bin: &Path) -> Result<Shadow, String> {
        let dir = std::env::temp_dir().join(format!("zou-shadow-{}", std::process::id()));
        // A leftover from a killed run of this same pid would be
        // initdb's problem, so it is this one's first.
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        let data = dir.join("data");
        let initdb = pg_bin.join("initdb");
        let out = Command::new(&initdb)
            .arg("-D")
            .arg(&data)
            .args(["-U", SUPERUSER])
            .args(["-A", "trust"])
            .args(["-E", "UTF8"])
            // Nothing in here is worth an fsync, it is deleted in a
            // moment either way, and this is most of the wall clock.
            .arg("--no-sync")
            .env_remove("ZOU_TARGET")
            .env_remove("ZOU_PAGESERVE")
            .output()
            .map_err(|e| {
                format!(
                    "cannot run {}: {e}, pass --pg-bin or set ZOU_PG_BIN",
                    initdb.display()
                )
            })?;
        if !out.status.success() {
            return Err(format!(
                "initdb for the shadow database failed:\n{}",
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        let log = std::fs::File::create(dir.join("postmaster.log"))
            .map_err(|e| format!("create the shadow log: {e}"))?;
        let postmaster = Command::new(pg_bin.join("postgres"))
            .arg("-D")
            .arg(&data)
            .arg("-k")
            .arg(&dir)
            // No tcp at all. The socket in the directory above is the
            // only way in, and it goes away with the directory.
            .args(["-c", "listen_addresses="])
            .args(["-c", "fsync=off"])
            .args(["-c", "full_page_writes=off"])
            .args(["-c", "synchronous_commit=off"])
            .args(["-c", "max_connections=8"])
            // Everything postgres has to say goes to the log next to the
            // socket, so a failure can be quoted and a working run is
            // silent.
            .args(["-c", "logging_collector=off"])
            .env_remove("ZOU_TARGET")
            .env_remove("ZOU_PAGESERVE")
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .map_err(|e| format!("cannot start the shadow postmaster: {e}"))?;
        let url = format!("host={} user={SUPERUSER} dbname=postgres", dir.display());
        Ok(Shadow {
            dir,
            postmaster: Some(postmaster),
            url,
        })
    }

    /// Wait for the postmaster to answer and hand back a connection to
    /// it. A cluster that has just been initdb'd refuses connections
    /// for a moment while it starts, which is a retry rather than a
    /// failure, and a postmaster that died says so in its log.
    pub async fn connect(&mut self) -> Result<Client, String> {
        let deadline = Instant::now() + READY;
        loop {
            match tokio_postgres::connect(&self.url, NoTls).await {
                Ok((client, connection)) => {
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            log::debug!("shadow connection ended: {e}");
                        }
                    });
                    return Ok(client);
                }
                Err(e) => {
                    if let Some(child) = &mut self.postmaster
                        && let Ok(Some(status)) = child.try_wait()
                    {
                        return Err(format!(
                            "the shadow postmaster stopped ({status}):\n{}",
                            self.log()
                        ));
                    }
                    if Instant::now() >= deadline {
                        return Err(format!(
                            "the shadow database did not answer in {}s: {e}\n{}",
                            READY.as_secs(),
                            self.log()
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    /// The last of what the postmaster wrote, for an error message.
    fn log(&self) -> String {
        let Ok(mut file) = std::fs::File::open(self.dir.join("postmaster.log")) else {
            return String::new();
        };
        let mut text = String::new();
        let _ = file.read_to_string(&mut text);
        text.lines()
            .rev()
            .take(10)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl Drop for Shadow {
    fn drop(&mut self) {
        if let Some(mut child) = self.postmaster.take() {
            // A postmaster does an immediate shutdown on SIGQUIT, which
            // is the one to send to something with nothing worth
            // writing out. SIGKILL would leave the shared memory
            // segment behind.
            unsafe {
                libc::kill(child.id() as libc::pid_t, libc::SIGQUIT);
            }
            let _ = child.wait();
        }
        if let Err(e) = std::fs::remove_dir_all(&self.dir) {
            log::debug!("could not remove {}: {e}", self.dir.display());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_binaries_come_from_the_flag_then_the_environment_then_the_build() {
        assert_eq!(
            pg_bin(Some(Path::new("/opt/pg/bin"))),
            Path::new("/opt/pg/bin")
        );
        // The environment is this process's, and the test suite runs
        // with it unset, so the fallback is what is left to check.
        if std::env::var_os("ZOU_PG_BIN").is_none() {
            assert_eq!(pg_bin(None), Path::new("build/pg/bin"));
        }
    }
}
