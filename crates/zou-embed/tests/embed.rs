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

/// The same thing branched off the machine's template, for a test that
/// wants a database rather than a store made from nothing. Seconds
/// cheaper each, once the template is there.
fn fixture() -> Option<Options> {
    pg_bin().map(|pg_bin| Options {
        pg_bin,
        ..Options::fixture()
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
    let Some(options) = fixture() else { return };
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
    let Some(options) = fixture() else { return };
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
    // Written after the last fold and committed before the branch is
    // asked for, which is the row a branch cut at the previous capture
    // would quietly leave behind.
    sql(&parent, "insert into public.rows values (9);");

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
    assert!(
        body(&answer).contains("\"id\":9"),
        "and so did the one written since the last fold, {}",
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
    // A branch of a project somebody keeps is a project somebody keeps.
    // Closing the handle stops a postmaster, it does not delete a
    // database, which is the opposite of what closing a branch of a
    // fixture does and is the reason the two are told apart at all.
    let store = zou_store::open_store(&dir.display().to_string()).expect("store");
    assert!(
        store
            .get(&zou_store::layout::TenantLayout::new("pr-42").manifest())
            .expect("store")
            .is_some(),
        "the branch is still on the store"
    );
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
fn a_fixture_is_branched_off_the_template_rather_than_made() {
    let Some(pg_bin) = pg_bin() else { return };
    // The first one on a cold machine builds the template, which is the
    // initdb every other one is not paying for.
    let built = std::time::Instant::now();
    let first = Zou::open(Options {
        pg_bin: pg_bin.clone(),
        ..Options::fixture()
    })
    .expect("open a fixture");
    eprintln!("the first fixture took {:?}", built.elapsed());

    let cut = std::time::Instant::now();
    let second = Zou::open(Options {
        pg_bin,
        ..Options::fixture()
    })
    .expect("open a second fixture");
    eprintln!("the second took {:?}", cut.elapsed());

    // The template folded before it was published, so a fixture starts
    // life with something a child could read inherited pages out of.
    // An ephemeral project has to be written to for a while first, and
    // that difference is the point of the template.
    assert!(
        first.branchable().expect("asking is cheap"),
        "a fixture can be branched the moment it is cut"
    );

    // Same store, two databases, and neither of them is the template.
    assert_eq!(first.target(), second.target(), "one template per machine");
    assert_ne!(first.tenant(), second.tenant());
    assert!(first.tenant().starts_with("fixture-"), "{}", first.tenant());

    // What says it was branched rather than initdb'd, which is the
    // whole point and is not a thing a stopwatch can prove on a loaded
    // machine.
    let store = zou_store::open_store(first.target()).expect("store");
    let key = zou_store::layout::TenantLayout::new(first.tenant()).manifest();
    let (data, _) = store
        .get(&key)
        .expect("store")
        .expect("the fixture is on the store");
    let manifest = zou_store::Manifest::from_json(&data).expect("manifest");
    let parent = manifest.branch_of.expect("a fixture has a parent");
    assert_eq!(parent.tenant_ref, "template");

    // Two databases, not two views of one.
    sql(
        &first,
        "create table public.mine (id int primary key);
         insert into public.mine values (1);
         grant select on public.mine to anon;",
    );
    let answer = first
        .request(
            "GET",
            "/rest/v1/mine",
            &[("apikey", first.keys().anon.as_str())],
            b"",
        )
        .expect("request");
    assert_eq!(answer.status, 200, "{}", body(&answer));
    assert!(body(&answer).contains("\"id\":1"), "{}", body(&answer));
    let elsewhere = second
        .request(
            "GET",
            "/rest/v1/mine",
            &[("apikey", second.keys().anon.as_str())],
            b"",
        )
        .expect("request");
    assert_eq!(
        elsewhere.status,
        404,
        "the other fixture has no such table, {}",
        body(&elsewhere)
    );

    // And closing takes the tenant off the store, leaving the template
    // it was cut from where it was.
    let target = first.target().to_string();
    let tenant = first.tenant().to_string();
    first.close().expect("close");
    second.close().expect("close");
    let store = zou_store::open_store(&target).expect("store");
    assert!(
        store
            .get(&zou_store::layout::TenantLayout::new(&tenant).manifest())
            .expect("store")
            .is_none(),
        "the fixture is gone"
    );
    assert!(
        store
            .get(&zou_store::layout::TenantLayout::new("template").manifest())
            .expect("store")
            .is_some(),
        "the template is not"
    );
}

/// A suite that wants its own schema in every test applies it once and
/// branches that, so the pattern the milestone asks for is a branch per
/// test rather than a fixture per test. The branch has to go off the
/// store when it closes the way the fixture it came from does: the
/// template store is the machine's, it is shared by every project on the
/// box, and a suite that leaves a database behind on every test leaves
/// thousands there by the end of the week.
#[test]
fn a_branch_of_a_fixture_goes_off_the_store_the_way_the_fixture_does() {
    let Some(pg_bin) = pg_bin() else { return };
    let base = Zou::open(Options {
        pg_bin,
        ..Options::fixture()
    })
    .expect("open a fixture");
    sql(
        &base,
        "create table public.seeded (id int primary key);
         insert into public.seeded values (1);
         grant select on public.seeded to anon;",
    );

    let child = base.branch("a-test-of-its-own").expect("branch");
    let anon = child.keys().anon.clone();
    let answer = child
        .request("GET", "/rest/v1/seeded", &[("apikey", &anon)], b"")
        .expect("request");
    assert_eq!(answer.status, 200, "{}", body(&answer));
    assert!(
        body(&answer).contains("\"id\":1"),
        "the seed came along, {}",
        body(&answer)
    );

    let target = base.target().to_string();
    let tenant = child.tenant().to_string();
    child.close().expect("close the child");
    let store = zou_store::open_store(&target).expect("store");
    assert!(
        store
            .get(&zou_store::layout::TenantLayout::new(&tenant).manifest())
            .expect("store")
            .is_none(),
        "the branch is off the template store"
    );

    // And the database it was cut from is still serving, because what
    // went is the child's own prefix and nothing it was reading through.
    let still = base
        .request(
            "GET",
            "/rest/v1/seeded",
            &[("apikey", base.keys().anon.as_str())],
            b"",
        )
        .expect("request");
    assert_eq!(still.status, 200, "{}", body(&still));
    base.close().expect("close the base");
}

#[test]
fn a_fixture_and_a_store_are_two_answers_to_the_same_question() {
    let Some(pg_bin) = pg_bin() else { return };
    let opened = Zou::open(Options {
        pg_bin,
        target: "/tmp/somewhere".to_string(),
        ..Options::fixture()
    });
    let Err(e) = opened else {
        panic!("a fixture lives on the template store, it cannot name another");
    };
    assert_eq!(e.kind, zou_embed::Kind::Options);
    assert!(e.message.contains("cannot name a store"), "{}", e.message);
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
