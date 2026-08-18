//! Stopping a node when the wal pusher is not there to help.
//!
//! The shutdown checkpoint runs after the postmaster has stopped every
//! background worker, the wal pusher included, so anything inside that
//! checkpoint that waits for the pusher waits for a process that has
//! already gone. That is zou #468: a node with a dirty buffer sat in
//! ZouWalWaitForLSN forever and nothing was left alive to end it.
//!
//! Both tests build that state deliberately. The pusher is stopped
//! first, a transaction dirties buffers and is left open so the
//! shutdown checkpoint has something to write, and then the node is
//! asked to stop. With the page service on the checkpointer stores no
//! page and so owes no barrier, and the node stops at once. With it off
//! the barrier is real, cannot ever be satisfied, and the checkpointer
//! says so and leaves rather than waiting.
//!
//! Gated on ZOU_PG_PREFIX, same as the other suites that need the
//! patched build:
//!
//!   ZOU_PG_PREFIX=$PWD/build/pg cargo test -p zou-pg --test shutdown

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Rows committed before the pusher goes, the ones an attach must find.
const COMMITTED: usize = 5000;

/// One node over one store: a data directory, a log, and a port that
/// only names a socket file, since nothing here listens on tcp.
struct Node {
    prefix: PathBuf,
    dir: PathBuf,
    data: PathBuf,
    store: PathBuf,
    log: PathBuf,
    port: u16,
    /// What the server gets for ZOU_PAGESERVE. The tools always run
    /// with it off: initdb and a restore have no service to read
    /// through, they are what puts the pages there in the first place.
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

    /// A command that sees the store, with the page service setting
    /// spelled out so no caller inherits one by accident.
    fn tool(&self, name: &str, pageserve: &str) -> Command {
        let mut cmd = Command::new(self.bin(name));
        cmd.env("ZOU_TARGET", &self.store)
            .env("ZOU_TENANT", "local")
            .env("ZOU_PAGESERVE", pageserve);
        cmd
    }

    /// A fresh cluster in the store, ready to be started.
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

    fn start(&self) {
        // A checkpoint of its own would write the dirty buffers these
        // tests are counting on, so the background writer is off and
        // the timeout is far away: the only checkpoint here is the one
        // the shutdown runs.
        let options = format!(
            "-p {} -k {} -c listen_addresses='' -c autovacuum=off \
             -c bgwriter_lru_maxpages=0 -c checkpoint_timeout=1h",
            self.port,
            self.dir.display()
        );
        run(self
            .tool("pg_ctl", self.pageserve)
            .arg("-D")
            .arg(&self.data)
            .arg("-l")
            .arg(&self.log)
            .arg("-o")
            .arg(options)
            .args(["-w", "-t", "120", "start"]));
    }

    fn psql_cmd(&self, sql: &str) -> Command {
        let mut cmd = Command::new(self.bin("psql"));
        cmd.arg("-h")
            .arg(&self.dir)
            .args(["-p", &self.port.to_string()])
            .args(["-U", "zou", "-d", "postgres", "-X", "-qAt", "-c", sql]);
        cmd
    }

    fn sql(&self, sql: &str) -> String {
        run(&mut self.psql_cmd(sql))
    }

    /// Stop fast and say whether the node stopped on its own, and how
    /// long it took. A false here is the bug: pg_ctl gave up.
    fn stop(&self, deadline: Duration) -> (bool, Duration) {
        let started = Instant::now();
        let out = self
            .tool("pg_ctl", self.pageserve)
            .arg("-D")
            .arg(&self.data)
            .args(["-m", "fast", "-w", "-t", &deadline.as_secs().to_string()])
            .arg("stop")
            .output()
            .expect("spawn pg_ctl stop");
        (out.status.success(), started.elapsed())
    }

    fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Wait for something the server logs, or say it never came.
    fn wait_log(&self, needle: &str, deadline: Duration) -> bool {
        let until = Instant::now() + deadline;
        while Instant::now() < until {
            if self.log_text().contains(needle) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        self.log_text().contains(needle)
    }

    /// The pusher's pid, out of the log line prefix of the line where
    /// it says it started mirroring. It is a background worker with no
    /// database connection, so pg_stat_activity has never heard of it.
    fn pusher_pid(&self) -> u32 {
        let text = self.log_text();
        let line = text
            .lines()
            .rev()
            .find(|line| line.contains("zou wal pusher mirroring"))
            .expect("the pusher says when it starts mirroring");
        let (_, rest) = line.split_once('[').expect("a pid in the line prefix");
        let (pid, _) = rest.split_once(']').expect("a pid in the line prefix");
        pid.parse().expect("the line prefix holds a pid")
    }

    fn control_state(&self) -> String {
        let control = run(self.tool("pg_controldata", "0").arg("-D").arg(&self.data));
        control
            .lines()
            .find(|line| line.contains("Database cluster state"))
            .and_then(|line| line.split(':').nth(1))
            .expect("control data names a cluster state")
            .trim()
            .to_string()
    }
}

/// Take the pusher out of the picture the way a shutdown does, and wait
/// until it has drained and gone.
fn stop_the_pusher(node: &Node) {
    assert!(
        node.wait_log("zou wal pusher mirroring", Duration::from_secs(60)),
        "the pusher came up"
    );
    let pid = node.pusher_pid();
    run(Command::new("kill").args(["-TERM", &pid.to_string()]));
    assert!(
        node.wait_log("zou wal pusher drained through", Duration::from_secs(120)),
        "the pusher drained and left, log:\n{}",
        node.log_text()
    );
}

/// A transaction that writes and never commits, so its pages are dirty
/// in the pool with LSNs past anything the store has, and the shutdown
/// checkpoint is the process that has to deal with them. Committing
/// would be a different test: a commit waits for the pusher itself.
fn dirty_buffers_and_leave_them(node: &Node) -> Child {
    let mut holder = node
        .psql_cmd(
            "begin; \
             insert into t select g, repeat('y', 200) from generate_series(90001, 95000) g; \
             select pg_sleep(600)",
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn the holding session");
    let until = Instant::now() + Duration::from_secs(60);
    while Instant::now() < until {
        let held = node.sql("select count(*) from pg_stat_activity where backend_xid is not null");
        if held.trim() != "0" {
            return holder;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = holder.kill();
    let _ = holder.wait();
    panic!("the holding session never took an xid");
}

/// The holding session goes when the node does, this only collects it.
fn release(mut holder: Child) {
    let _ = holder.kill();
    let _ = holder.wait();
}

fn prefix() -> Option<PathBuf> {
    match std::env::var("ZOU_PG_PREFIX") {
        Ok(v) if !v.is_empty() => Some(PathBuf::from(v)),
        _ => {
            eprintln!("ZOU_PG_PREFIX not set, skipping the shutdown tests");
            None
        }
    }
}

fn workload(node: &Node) {
    node.sql("create table t(id int primary key, v text)");
    node.sql(&format!(
        "insert into t select g, repeat('x', 200) from generate_series(1, {COMMITTED}) g"
    ));
}

#[test]
fn a_node_stops_by_itself_with_dirty_buffers_and_no_pusher() {
    let Some(prefix) = prefix() else { return };
    let tmp = tempfile::tempdir().expect("tempdir");
    let node = Node::new(&prefix, tmp.path(), "data", 54681, "1");
    node.create();
    node.start();
    workload(&node);

    stop_the_pusher(&node);
    let holder = dirty_buffers_and_leave_them(&node);

    // The shutdown checkpoint has pages to write and no pusher to ask
    // about them. Before zou #468 it waited here for good.
    let (stopped, took) = node.stop(Duration::from_secs(60));
    release(holder);
    assert!(
        stopped,
        "the node stopped on its own after {took:?}, log:\n{}",
        node.log_text()
    );
    assert!(
        took < Duration::from_secs(30),
        "the shutdown was prompt, not a grace period running out: {took:?}"
    );
    assert_eq!(node.control_state(), "shut down", "a clean stop");

    // And the store the node leaves behind is one a fresh node can pick
    // up: every committed row, and a tail short enough that the attach
    // is a start rather than a recovery.
    let attached = Node::new(&prefix, tmp.path(), "data2", 54683, "1");
    run(Command::new(env!("CARGO_BIN_EXE_zou-restore"))
        .env("ZOU_PAGESERVE", "0")
        .arg(&attached.store)
        .arg(&attached.data)
        .arg("local"));
    attached.start();
    assert_eq!(
        attached.sql("select count(*) from t"),
        COMMITTED.to_string(),
        "the attached node has every committed row"
    );
    let (stopped, _) = attached.stop(Duration::from_secs(60));
    assert!(stopped, "the attached node stops too");
}

#[test]
fn a_checkpoint_that_cannot_be_made_durable_says_so_and_leaves() {
    let Some(prefix) = prefix() else { return };
    let tmp = tempfile::tempdir().expect("tempdir");
    // With the page service off the checkpointer stores its pages
    // itself, so the barrier before the put is owed and there is no
    // eliding it. It cannot be paid either, which is the case for
    // giving up on a wait instead of sleeping through it.
    let node = Node::new(&prefix, tmp.path(), "data", 54685, "0");
    node.create();
    node.start();
    workload(&node);

    stop_the_pusher(&node);
    let holder = dirty_buffers_and_leave_them(&node);

    let (stopped, took) = node.stop(Duration::from_secs(120));
    release(holder);
    assert!(
        stopped,
        "the node gave up and stopped after {took:?}, log:\n{}",
        node.log_text()
    );
    assert!(
        node.log_text().contains("no pusher to make"),
        "it says why it could not finish, log:\n{}",
        node.log_text()
    );
}
