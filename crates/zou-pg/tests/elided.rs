//! Reopening on the object path a store the page service has run on.
//!
//! With the page service on the eager put per page write is elided, so
//! the objects under pg/ stop where that session started while the
//! checkpoints keep advancing on the layers above them. Reading those
//! objects again with the service off then puts recovery at a redo
//! location far past the pages it is applying records to, and the first
//! heap record to land on one brings the postmaster down with "invalid
//! lp". That is zou #462, and the answer is a refusal that says so.
//!
//! Gated on ZOU_PG_PREFIX, same as the other suites that need the
//! patched build:
//!
//!   ZOU_PG_PREFIX=$PWD/build/pg cargo test -p zou-pg --test elided

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use zou_store::Manifest;

/// Rows to write, enough to dirty pages the elision then skips.
const ROWS: usize = 5_000;

/// One node over one store, the shape the shutdown suite uses: a data
/// directory, a log, and a port that only names a socket file.
struct Node {
    prefix: PathBuf,
    dir: PathBuf,
    data: PathBuf,
    store: PathBuf,
    log: PathBuf,
    port: u16,
    /// What the server gets for ZOU_PAGESERVE. The tools always run
    /// with it off, they have no service to read through.
    pageserve: &'static str,
}

fn run(cmd: &mut Command) -> String {
    let out = cmd
        .output()
        .unwrap_or_else(|err| panic!("spawn {cmd:?}: {err}"));
    assert!(
        out.status.success(),
        "{cmd:?} failed: {}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

impl Node {
    fn new(prefix: &Path, dir: &Path, name: &str, port: u16, pageserve: &'static str) -> Node {
        Node {
            prefix: prefix.to_path_buf(),
            dir: dir.to_path_buf(),
            data: dir.join(name),
            store: dir.join("store"),
            log: dir.join(format!("{name}.log")),
            port,
            pageserve,
        }
    }

    fn bin(&self, name: &str) -> PathBuf {
        self.prefix.join("bin").join(name)
    }

    fn tool(&self, name: &str, pageserve: &str) -> Command {
        let mut cmd = Command::new(self.bin(name));
        cmd.env("ZOU_TARGET", &self.store)
            .env("ZOU_TENANT", "local")
            .env("ZOU_PAGESERVE", pageserve);
        cmd
    }

    /// A fresh cluster in the store, bootstrapped with every page
    /// written, which is the state the object path needs.
    fn create(&self) {
        std::fs::create_dir_all(&self.store).expect("store dir");
        run(self
            .tool("initdb", "0")
            .args(["--no-sync", "-A", "trust", "-U", "zou"])
            .args(["--set", "io_method=sync", "--set", "full_page_writes=off"])
            .arg("-D")
            .arg(&self.data));
        let control = run(self.tool("pg_controldata", "0").arg("-D").arg(&self.data));
        let redo = control
            .lines()
            .find(|line| line.contains("REDO location"))
            .and_then(|line| line.split(':').nth(1))
            .expect("control data names a redo location")
            .trim()
            .to_string();
        run(Command::new(env!("CARGO_BIN_EXE_zou-bootstrap"))
            .env("ZOU_PAGESERVE", "0")
            .arg(&self.store)
            .arg(&self.data)
            .args(["--redo", &redo]));
    }

    fn options(&self) -> String {
        format!(
            "-p {} -k {} -c listen_addresses='' -c autovacuum=off",
            self.port,
            self.dir.display()
        )
    }

    fn start(&self) {
        run(self
            .tool("pg_ctl", self.pageserve)
            .arg("-D")
            .arg(&self.data)
            .arg("-l")
            .arg(&self.log)
            .arg("-o")
            .arg(self.options())
            .args(["-w", "-t", "120", "start"]));
    }

    /// Start and expect a refusal: the exit status and the log the
    /// postmaster wrote on its way out.
    fn start_expecting_failure(&self) -> String {
        let out = self
            .tool("pg_ctl", self.pageserve)
            .arg("-D")
            .arg(&self.data)
            .arg("-l")
            .arg(&self.log)
            .arg("-o")
            .arg(self.options())
            .args(["-w", "-t", "60", "start"])
            .output()
            .expect("spawn pg_ctl start");
        assert!(
            !out.status.success(),
            "the node started, log:\n{}",
            self.log_text()
        );
        self.log_text()
    }

    fn sql(&self, sql: &str) -> String {
        run(Command::new(self.bin("psql"))
            .arg("-h")
            .arg(&self.dir)
            .args(["-p", &self.port.to_string()])
            .args(["-U", "zou", "-d", "postgres", "-X", "-qAt", "-c", sql]))
    }

    fn stop(&self) {
        let out = self
            .tool("pg_ctl", self.pageserve)
            .arg("-D")
            .arg(&self.data)
            .args(["-m", "fast", "-w", "-t", "120"])
            .arg("stop")
            .output()
            .expect("spawn pg_ctl stop");
        assert!(
            out.status.success(),
            "the node stopped, log:\n{}",
            self.log_text()
        );
    }

    fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn manifest(&self) -> Manifest {
        let path = self.store.join("tenants/local/MANIFEST");
        let data = std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        Manifest::from_json(&data).expect("the store holds a manifest")
    }

    /// Wait for the fold the pusher runs after a checkpoint, so the
    /// store's newest capture is past the point the pages froze.
    fn wait_for_a_capture_past(&self, elided: u64) {
        let until = Instant::now() + Duration::from_secs(60);
        while Instant::now() < until {
            if self
                .manifest()
                .captured_upto()
                .is_some_and(|lsn| lsn.0 > elided)
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        panic!(
            "no capture past {elided:#X}, manifest: {:?}",
            self.manifest()
        );
    }
}

fn prefix() -> Option<PathBuf> {
    match std::env::var("ZOU_PG_PREFIX") {
        Ok(v) if !v.is_empty() => Some(PathBuf::from(v)),
        _ => {
            eprintln!("ZOU_PG_PREFIX not set, skipping the elision tests");
            None
        }
    }
}

#[test]
fn a_store_the_page_service_has_run_on_refuses_the_object_path() {
    let Some(prefix) = prefix() else { return };
    let tmp = tempfile::tempdir().expect("tempdir");

    // One session with the service on, which is the default and the
    // whole of what it takes: from the moment it opens, the pages under
    // pg/ are the ones bootstrap wrote and nothing else.
    let node = Node::new(&prefix, tmp.path(), "data", 54691, "1");
    node.create();
    assert_eq!(
        node.manifest().pages_elided_from,
        None,
        "a bootstrapped store has written every page it holds"
    );
    node.start();
    node.sql("create table t(id int primary key, v text)");
    node.sql(&format!(
        "insert into t select g, repeat('x', 200) from generate_series(1, {ROWS}) g"
    ));
    node.sql("checkpoint");
    let elided = node
        .manifest()
        .pages_elided_from
        .expect("the session marked where the eager puts stopped");
    node.wait_for_a_capture_past(elided.0);
    node.stop();

    // Same store, same data directory, object path. Before zou #462
    // this was three postmasters and a PANIC in redo.
    let off = Node::new(&prefix, tmp.path(), "data", 54691, "0");
    let log = off.start_expecting_failure();
    assert!(
        log.contains("no page objects past"),
        "the refusal says what it cannot read, log:\n{log}"
    );
    assert!(
        log.contains("only ever run with it off"),
        "the refusal says what a comparison needs instead, log:\n{log}"
    );

    // And the store is not broken, only unreadable that one way.
    let on = Node::new(&prefix, tmp.path(), "data", 54691, "1");
    on.start();
    assert_eq!(on.sql("select count(*) from t"), ROWS.to_string());
    on.stop();
}

#[test]
fn a_store_that_has_only_ever_run_on_the_object_path_opens_on_it() {
    let Some(prefix) = prefix() else { return };
    let tmp = tempfile::tempdir().expect("tempdir");

    let node = Node::new(&prefix, tmp.path(), "data", 54693, "0");
    node.create();
    node.start();
    node.sql("create table t(id int primary key, v text)");
    node.sql(&format!(
        "insert into t select g, repeat('x', 200) from generate_series(1, {ROWS}) g"
    ));
    node.sql("checkpoint");
    node.stop();
    assert_eq!(
        node.manifest().pages_elided_from,
        None,
        "nothing elided anything"
    );

    node.start();
    assert_eq!(node.sql("select count(*) from t"), ROWS.to_string());
    node.stop();
}
