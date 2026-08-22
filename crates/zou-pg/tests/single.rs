//! The one shot backend against the real thing, on a real store.
//!
//! [`zou_pg::single`] parses what `postgres --single` prints, and the
//! fixtures its unit tests read were transcribed off a run by hand.
//! Transcriptions rot: the day the backend adds a field to its type
//! description or stops leaving nulls out, those fixtures still parse
//! and still pass and say nothing about the postgres in the bundle.
//! This test asks the postgres in the bundle the same questions,
//! attached to a zou store rather than to a directory, and checks the
//! answers, so the parser is held against the binary rather than
//! against a memory of it.
//!
//! Gated on ZOU_PG_PREFIX exactly like the other integration tests
//! here: without the patched build it prints a skip note and passes.

use std::path::{Path, PathBuf};
use std::process::Command;

use zou_pg::single::Session;

/// A pristine cluster whose pages live in `store`, laid out the way
/// `zou dev` lays one out: sync io because the storage manager requires
/// it, and no full page writes because a put is atomic.
fn initdb(prefix: &Path, datadir: &Path, store: &Path) {
    let out = Command::new(prefix.join("bin").join("initdb"))
        .args(["--no-sync", "-U", "postgres"])
        .args(["--set", "io_method=sync"])
        .args(["--set", "full_page_writes=off"])
        .arg("-D")
        .arg(datadir)
        .env("ZOU_TARGET", store)
        .env("ZOU_TENANT", "local")
        // initdb is a standalone process, there is no page service to
        // read through, so the object path is what this exercises.
        .env("ZOU_PAGESERVE", "0")
        .output()
        .expect("spawn initdb");
    assert!(
        out.status.success(),
        "initdb failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn the_backend_answers_what_the_parser_expects_it_to() {
    let Ok(prefix) = std::env::var("ZOU_PG_PREFIX") else {
        eprintln!("ZOU_PG_PREFIX not set, skipping the single user backend test");
        return;
    };
    let prefix = PathBuf::from(prefix);
    let tmp = tempfile::tempdir().expect("tempdir");
    let datadir = tmp.path().join("data");
    let store = tmp.path().join("store");
    std::fs::create_dir_all(&store).expect("store dir");
    initdb(&prefix, &datadir, &store);

    let session = || {
        Session::new(&prefix.join("bin"), &datadir)
            .env("ZOU_TARGET", &store)
            .env("ZOU_TENANT", "local")
            .env("ZOU_PAGESERVE", "0")
    };

    // The null in the middle is the whole point. The backend prints no
    // field at all for it, so a row that reads back with the third
    // value under the third name is a row that was read by column
    // number rather than by counting what arrived.
    let sets = session()
        .run("select 1 as one, null::text as n, 'x y' as s;")
        .expect("query");
    assert_eq!(sets.len(), 1, "one statement, one answer");
    assert_eq!(sets[0].columns, ["one", "n", "s"]);
    assert_eq!(sets[0].get(0, "one"), Some("1"));
    assert_eq!(sets[0].get(0, "n"), None);
    assert_eq!(sets[0].get(0, "s"), Some("x y"));

    // Several statements, several rows, and a value carrying the quote
    // character the backend does not escape.
    let sets = session()
        .run(
            "select * from (values (1, 'q\"uote'), (2, 'plain')) v(a, b);\n\
             select 3 as after;\n",
        )
        .expect("query");
    assert_eq!(sets.len(), 2);
    assert_eq!(sets[0].rows.len(), 2);
    assert_eq!(sets[0].get(0, "b"), Some("q\"uote"));
    assert_eq!(sets[0].get(1, "b"), Some("plain"));
    assert_eq!(sets[1].scalar(), Some("3"));

    // A write needs asking for, and the refusal is at the statement
    // rather than at the api, because that is where a caller who set
    // out to write finds out that a write from here would be lost.
    let refused = session()
        .run("create table t (id int primary key);")
        .expect_err("a session is read only unless it says otherwise");
    assert!(
        refused.contains("read-only transaction"),
        "the refusal should be the read only one, got {refused:?}"
    );

    // A write in one session is in the store for the next one, which
    // holds here for the reason issue #548 sets out: this store has
    // never been captured, so there is no checkpoint run holding an
    // older copy of the catalog pages this write changes. On a store
    // with a checkpoint in it the same two sessions would disagree.
    session()
        .writable()
        .run("create table t (id int primary key); insert into t values (7);")
        .expect("write");
    let sets = session()
        .run("select count(*) as n from t;")
        .expect("count");
    assert_eq!(sets[0].scalar(), Some("1"));

    // And once the store has been captured, the same write is refused
    // before the backend starts. This is issue #548: the pages a one
    // shot session writes would be shadowed by the newest run on the
    // next attach, because nothing here pushes its wal into the shared
    // log for the chain to replay.
    //
    // The manifest is written here rather than captured. initdb leaves
    // none at all, which is itself the bootstrap window the two
    // sessions above just wrote through, and what the refusal reads is
    // the count of checkpoints in the manifest and nothing else.
    let layout = zou_store::layout::TenantLayout::new("local");
    let backing = zou_store::open_store(store.to_str().unwrap()).expect("open the store");
    assert!(
        backing
            .get(&layout.manifest())
            .expect("read the manifest")
            .is_none(),
        "initdb leaves no manifest, which is why the write above held"
    );
    let mut manifest = zou_store::Manifest::new("local", 18);
    manifest
        .checkpoints
        .push(zou_store::manifest::CheckpointRef {
            id: "pretend".into(),
            lsn: zou_store::Lsn(0x100),
            kind: zou_store::manifest::CheckpointKind::Full,
            owner: None,
        });
    backing
        .put(&layout.manifest(), &manifest.to_json())
        .expect("write the manifest");
    let refused = session()
        .writable()
        .run("insert into t values (8);")
        .expect_err("a captured store refuses a one shot writer");
    assert!(
        refused.contains("one shot backend may not write"),
        "the refusal should name the reason, got {refused:?}"
    );
    // Reading is untouched by it, which is what the one production
    // caller of a session does.
    let sets = session()
        .run("select count(*) as n from t;")
        .expect("count");
    assert_eq!(sets[0].scalar(), Some("1"));

    // The failure the exit status does not report. The backend logs
    // ERROR, runs the statement after it, and exits 0, so a caller that
    // trusted the status would read the empty result as a clean run.
    let failed = session()
        .run("select nosuch; select 1 as ran_anyway;")
        .expect_err("an error is an error");
    assert!(
        failed.contains("nosuch"),
        "the refusal should say what the backend said, got {failed:?}"
    );
}
