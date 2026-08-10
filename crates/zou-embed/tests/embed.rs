//! An embedded project, opened for real.
//!
//! Gated on ZOU_PG_BIN naming a patched install, same shape as the
//! other suites that need something the plain build does not have.
//! Unset means every test here skips and `cargo test` stays offline:
//!
//!   ZOU_PG_BIN=$PWD/build/pg/bin cargo test -p zou-embed --test embed
//!
//! These start postgres, so they are seconds each rather than
//! microseconds. What they are checking is the three things a host
//! process cannot check for itself: that a request answered in process
//! is the same request the port would have answered, that a branch
//! carries what the parent wrote and then goes its own way, and that
//! closing the handle takes the postmaster with it.

#![cfg(unix)]

use std::io::{Read, Write};
use std::path::PathBuf;

use zou_embed::{Options, Zou};

fn pg_bin() -> Option<PathBuf> {
    match std::env::var("ZOU_PG_BIN") {
        Ok(v) if !v.is_empty() => Some(PathBuf::from(v)),
        _ => {
            eprintln!("skipping: ZOU_PG_BIN not set");
            None
        }
    }
}

fn options() -> Option<Options> {
    pg_bin().map(|pg_bin| Options {
        pg_bin,
        ..Options::ephemeral()
    })
}

/// Somewhere for a test that needs a store that outlives one handle.
fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("zou-embed-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch");
    dir
}

/// Run some sql as the superuser, which is how a host process sets its
/// own schema up before serving it.
fn sql(zou: &Zou, statements: &str) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let (client, connection) = tokio_postgres::connect(zou.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        let pump = tokio::spawn(connection);
        client.batch_execute(statements).await.expect("sql");
        drop(client);
        let _ = pump.await;
    });
}

fn body(answer: &zou_embed::Response) -> String {
    String::from_utf8_lossy(&answer.body).into_owned()
}

#[test]
fn a_project_answers_in_process_and_still_checks_who_is_asking() {
    let Some(options) = options() else { return };
    let zou = Zou::open(options).expect("open");
    sql(
        &zou,
        "create table public.todos (id int primary key, title text);
         insert into public.todos values (1, 'write the binding');
         grant select on public.todos to anon;",
    );

    let anon = zou.keys().anon.clone();
    let answer = zou
        .request(
            "GET",
            "/rest/v1/todos?select=title",
            &[("apikey", &anon)],
            b"",
        )
        .expect("request");
    assert_eq!(answer.status, 200, "{}", body(&answer));
    assert!(
        body(&answer).contains("write the binding"),
        "{}",
        body(&answer)
    );
    assert_eq!(
        answer.header("content-type"),
        Some("application/json; charset=utf-8")
    );

    // Nothing is added on the way in, so a call with no key is refused
    // exactly as it would be over http.
    let refused = zou
        .request("GET", "/rest/v1/todos", &[], b"")
        .expect("request");
    assert_eq!(refused.status, 401, "{}", body(&refused));
}

#[test]
fn the_same_project_is_reachable_over_a_port_as_well() {
    let Some(options) = options() else { return };
    let zou = Zou::open(options).expect("open");
    sql(
        &zou,
        "create table public.notes (id int primary key);
         insert into public.notes values (7);
         grant select on public.notes to anon;",
    );
    let port = zou.listen(0).expect("listen");
    assert_ne!(port, 0, "the kernel named one");

    let mut sock = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let request = format!(
        "GET /rest/v1/notes HTTP/1.1\r\nHost: 127.0.0.1\r\napikey: {}\r\nConnection: close\r\n\r\n",
        zou.keys().anon
    );
    sock.write_all(request.as_bytes()).expect("write");
    let mut seen = String::new();
    sock.read_to_string(&mut seen).expect("read");
    assert!(seen.starts_with("HTTP/1.1 200"), "{seen}");
    assert!(seen.contains("\"id\":7"), "{seen}");
}

/// Write until a fold has packed a full page capture down, which is
/// what a child reads inherited pages out of. A database three rows old
/// has not done that yet and one that has been serving for a while did
/// it long ago, so a test that wants a branch has to do the work the
/// demo does.
fn settle(zou: &Zou) {
    for _ in 0..20 {
        sql(
            zou,
            "insert into public.settling(pad) select repeat('x', 80) from generate_series(1, 2000);
             checkpoint;",
        );
        std::thread::sleep(std::time::Duration::from_secs(2));
        if zou.branchable().expect("asking is cheap") {
            return;
        }
    }
    panic!("no full capture was folded, a branch would be refused");
}

#[test]
fn a_branch_carries_what_the_parent_had_and_then_goes_its_own_way() {
    let Some(pg_bin) = pg_bin() else { return };
    let dir = scratch("branch");
    let parent = Zou::open(Options {
        pg_bin: pg_bin.clone(),
        ..Options::dir(&dir)
    })
    .expect("open");
    sql(
        &parent,
        "create table public.settling (id serial primary key, pad text);
         create table public.rows (id int primary key);
         insert into public.rows values (1);
         grant select on public.rows to anon;",
    );
    settle(&parent);

    let child = parent.branch("pr-42").expect("branch");
    assert_eq!(child.tenant(), "pr-42");
    let anon = child.keys().anon.clone();
    let answer = child
        .request("GET", "/rest/v1/rows", &[("apikey", &anon)], b"")
        .expect("request");
    assert_eq!(answer.status, 200, "{}", body(&answer));
    assert!(
        body(&answer).contains("\"id\":1"),
        "the parent's row came along, {}",
        body(&answer)
    );

    // Two databases now, not two views of one.
    sql(&child, "insert into public.rows values (2);");
    sql(&parent, "insert into public.rows values (3);");
    let from_child = child
        .request("GET", "/rest/v1/rows?order=id", &[("apikey", &anon)], b"")
        .expect("request");
    let from_parent = parent
        .request(
            "GET",
            "/rest/v1/rows?order=id",
            &[("apikey", parent.keys().anon.as_str())],
            b"",
        )
        .expect("request");
    assert!(
        body(&from_child).contains("\"id\":2"),
        "{}",
        body(&from_child)
    );
    assert!(
        !body(&from_child).contains("\"id\":3"),
        "{}",
        body(&from_child)
    );
    assert!(
        body(&from_parent).contains("\"id\":3"),
        "{}",
        body(&from_parent)
    );
    assert!(
        !body(&from_parent).contains("\"id\":2"),
        "{}",
        body(&from_parent)
    );

    child.close().expect("close the child");
    parent.close().expect("close the parent");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_database_too_young_to_serve_a_branch_says_so_and_leaves_nothing_behind() {
    let Some(pg_bin) = pg_bin() else { return };
    let dir = scratch("young");
    let zou = Zou::open(Options {
        pg_bin,
        ..Options::dir(&dir)
    })
    .expect("open");
    assert!(!zou.branchable().expect("asking is cheap"));
    let Err(e) = zou.branch("too-soon") else {
        panic!("a database this young has no full capture to inherit");
    };
    assert!(
        e.message.contains("cannot be branched yet"),
        "{}",
        e.message
    );
    let store = zou_store::open_store(&dir.display().to_string()).expect("store");
    let manifest = zou_store::layout::TenantLayout::new("too-soon").manifest();
    assert!(
        store.get(&manifest).expect("store").is_none(),
        "the child was taken back off the store"
    );
    zou.close().expect("close");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn what_the_parent_committed_is_on_the_store_before_the_branch_is_cut() {
    let Some(pg_bin) = pg_bin() else { return };
    let dir = scratch("reopen");
    let first = Zou::open(Options {
        pg_bin: pg_bin.clone(),
        ..Options::dir(&dir)
    })
    .expect("open");
    sql(
        &first,
        "create table public.kept (id int primary key);
         insert into public.kept values (11);
         grant select on public.kept to anon;",
    );
    first.checkpoint().expect("checkpoint");
    first.close().expect("close");

    // A second handle over the same store is the restore path, and it
    // is the one thing that says the running copy was never the data.
    let again = Zou::open(Options {
        pg_bin,
        ..Options::dir(&dir)
    })
    .expect("reopen");
    let answer = again
        .request(
            "GET",
            "/rest/v1/kept",
            &[("apikey", again.keys().anon.as_str())],
            b"",
        )
        .expect("request");
    assert_eq!(answer.status, 200, "{}", body(&answer));
    assert!(body(&answer).contains("\"id\":11"), "{}", body(&answer));
    again.close().expect("close");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn closing_takes_the_postmaster_and_the_running_copy_with_it() {
    let Some(options) = options() else { return };
    let runtime = scratch("close");
    let zou = Zou::open(Options {
        runtime: Some(runtime.clone()),
        ..options
    })
    .expect("open");
    let store = PathBuf::from(zou.target());
    assert!(
        store.is_dir(),
        "the ephemeral store is under the runtime dir"
    );
    zou.close().expect("close");
    assert!(!runtime.exists(), "the running copy is gone");
}
