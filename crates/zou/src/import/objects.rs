//! The bytes behind `storage.objects`, which the row copy next door
//! does not move.
//!
//! A storage object is two things in two places: a row in
//! `storage.objects` that says which bucket it is in, what it is
//! called and how big it is, and the bytes themselves, which on a
//! hosted project live behind the storage api and here live in the
//! object store under `tenants/<ref>/files/objects/<id>/<version>`.
//! The rows come over with the rest of the database. The bytes have to
//! be fetched one at a time over http, which is slow enough that this
//! is a separate step with its own ledger and its own resume.
//!
//! The ledger here is an optimisation rather than a correctness
//! requirement, which is the opposite of the one in `copy.rs`. A key is
//! `id` and `version` out of the row, both of which the source chose
//! and neither of which this changes, so writing the same object twice
//! writes the same bytes to the same key. Losing the ledger costs a
//! second download and nothing else. That is why the ledger is written
//! a chunk at a time instead of once per object: a kill in the middle
//! of a chunk repeats at most that chunk.
//!
//! Every object that did not come over is named. A row whose bytes are
//! gone on the far side answers 404, and rather than stopping the run,
//! which would leave every later object unfetched over one deleted
//! file, it is written down and the run carries on. A wrong key answers
//! 401 and does stop, because every remaining object would answer the
//! same way.
//!
//! The manifest is written from the ledger rather than from this run,
//! so a resumed run still writes a manifest covering every object and
//! not just the ones it happened to fetch.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tokio_postgres::Client;
use zou_store::layout::TenantLayout;
use zou_store::{CasStore, open_store};

use super::copy::why;

/// The tenant a single node server keeps its own database under, which
/// is where an import with no `--tenant` puts the objects. Spelled out
/// rather than imported because `zou-server` is only a dependency on
/// unix and this command is not, and a test down the file holds the two
/// together where the crate is there to ask.
pub const DEFAULT_TENANT: &str = "local";

/// Enough downloads in flight to fill a link to one host, few enough
/// that a project with a hundred thousand small objects does not look
/// like an attack from the other end.
pub const DEFAULT_JOBS: usize = 8;

/// How many objects go between ledger writes. A kill costs at most one
/// of these in repeated downloads, and the ledger costs one round trip
/// per chunk instead of one per object.
const CHUNK: usize = 256;

/// Statuses worth another try: throttling and the transient half of
/// the 5xx range. Everything else means what it says.
fn retryable(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

const ATTEMPTS: u32 = 3;

/// The ledger. In the `zou` schema with the step ledger, for the same
/// reason: a `zou db diff` reads that schema as the server's and does
/// not try to write a migration for it.
const LEDGER: &str = "
set client_min_messages = warning;
create schema if not exists zou;
create table if not exists zou.import_objects (
    id text primary key,
    version text not null,
    bucket text not null,
    name text not null,
    bytes bigint not null,
    sha256 text not null,
    at timestamptz not null default now()
);
reset client_min_messages;
";

/// Every object row, in an order that makes the manifest readable. The
/// size is the one the project recorded, which this checks the bytes
/// against rather than trusting.
const ROWS_SQL: &str = "\
select o.id::text,
       coalesce(o.version, ''),
       coalesce(o.bucket_id, ''),
       coalesce(o.name, ''),
       coalesce((o.metadata->>'size')::bigint, -1)
from storage.objects o
order by o.bucket_id, o.name";

/// Where the bytes come from and where they go.
#[derive(Debug)]
pub struct Where {
    /// The store this server keeps its objects in.
    pub store: String,
    /// Which tenant of it.
    pub tenant: String,
    /// The storage api of the project being left, up to and including
    /// `/storage/v1`.
    pub base: String,
    /// The service role key, which is what reads a private bucket.
    pub key: String,
    pub jobs: usize,
    pub manifest: PathBuf,
}

/// One object as the database describes it.
#[derive(Debug, Clone)]
struct Object {
    id: String,
    version: String,
    bucket: String,
    name: String,
    /// What the project recorded, or -1 when it recorded nothing.
    said: i64,
}

impl Object {
    /// Where the bytes go. This is `blob::key` in `zou-server` with the
    /// tenant prefix in front of it, and the test at the bottom of this
    /// file holds the two spellings together.
    fn key(&self, tenant: &str) -> String {
        format!(
            "{}objects/{}/{}",
            TenantLayout::new(tenant).files_prefix(),
            self.id,
            self.version
        )
    }

    fn path(&self) -> String {
        format!("{}/{}", self.bucket, self.name)
    }
}

/// What one object's turn came to.
enum Fetched {
    /// Bytes, and their digest.
    Bytes(Vec<u8>, String),
    /// The row is here and the bytes are not, with what the far side
    /// said about it.
    Gone(String),
}

/// What one run of this step did.
#[derive(Debug, Default)]
pub struct Moved {
    pub copied: usize,
    pub already: usize,
    pub bytes: u64,
    /// Rows whose bytes did not come, each with the reason. Never
    /// summarised away: a project that lost a file to a half deleted
    /// bucket years ago should read that here and not find out from a
    /// broken image.
    pub gone: Vec<String>,
    /// Objects whose bytes are not the size the project recorded. The
    /// bytes are copied anyway, because they are the thing that is
    /// real, and the disagreement is written down.
    pub mismatched: Vec<String>,
    pub manifest: PathBuf,
    pub seconds: f64,
}

impl Moved {
    pub fn render(&self) -> String {
        let mut out = format!(
            "{} object bytes copied, {} already there, {} in {:.1}s\n",
            self.copied,
            self.already,
            super::size(self.bytes as i64),
            self.seconds
        );
        for line in &self.gone {
            out.push_str(&format!("no bytes for {line}\n"));
        }
        for line in &self.mismatched {
            out.push_str(&format!("size disagrees for {line}\n"));
        }
        out.push_str(&format!(
            "manifest written to {}\n",
            self.manifest.display()
        ));
        out
    }
}

/// The storage api of a hosted project, which is the one place its url
/// is spelled so that a change to it is a change to one line.
pub fn base_for(project_ref: &str) -> String {
    format!("https://{project_ref}.supabase.co/storage/v1")
}

/// Percent encoding for a path, keeping `/` because a storage object's
/// name is a path and its slashes are separators on both sides.
fn encode_path(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for b in raw.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn url_for(base: &str, o: &Object) -> String {
    format!(
        "{}/object/authenticated/{}/{}",
        base.trim_end_matches('/'),
        encode_path(&o.bucket),
        encode_path(&o.name)
    )
}

fn digest(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// One object off the far side, with the retries the network asks for.
///
/// A 404 comes back as `Gone` rather than an error because a row
/// without its bytes is a fact about the project and not a reason to
/// abandon the other ninety thousand objects. Everything else that is
/// not a 200 is an error, and a wrong key is the reason: it answers
/// the same way for every object, so stopping on the first one saves
/// somebody watching a progress bar count to a hundred thousand
/// failures.
fn fetch(agent: &ureq::Agent, w: &Where, o: &Object) -> Result<Fetched, String> {
    let url = url_for(&w.base, o);
    let mut attempt = 0;
    loop {
        attempt += 1;
        let sent = agent
            .get(&url)
            .header("authorization", &format!("Bearer {}", w.key))
            .header("apikey", &w.key)
            .call();
        let response = match sent {
            Ok(response) => response,
            Err(e) if attempt < ATTEMPTS => {
                std::thread::sleep(Duration::from_millis(200) * 2u32.pow(attempt - 1));
                log::warn!("{}: {e}, trying again", o.path());
                continue;
            }
            Err(e) => return Err(format!("{}: {e}", o.path())),
        };
        let status = response.status().as_u16();
        if retryable(status) && attempt < ATTEMPTS {
            std::thread::sleep(Duration::from_millis(200) * 2u32.pow(attempt - 1));
            continue;
        }
        if status == 404 {
            return Ok(Fetched::Gone("the storage api answered 404".into()));
        }
        if status != 200 {
            return Err(format!("{}: the storage api answered {status}", o.path()));
        }
        let mut data = Vec::new();
        std::io::copy(&mut response.into_body().into_reader(), &mut data)
            .map_err(|e| format!("{}: reading the body: {e}", o.path()))?;
        let sha = digest(&data);
        return Ok(Fetched::Bytes(data, sha));
    }
}

/// One object fetched and written, or a reason it was not.
struct Outcome {
    object: Object,
    result: Result<Fetched, String>,
}

/// A chunk of objects, in parallel, each one fetched and then written
/// to the store. The postgres client is not touched in here, which is
/// what lets this be threads: the ledger for the chunk is written by
/// the caller afterwards.
fn chunk(
    agent: &ureq::Agent,
    store: &Arc<dyn CasStore>,
    w: &Where,
    batch: &[Object],
) -> Vec<Outcome> {
    if batch.is_empty() {
        return Vec::new();
    }
    let next = AtomicUsize::new(0);
    let out: Vec<std::sync::Mutex<Option<Outcome>>> =
        batch.iter().map(|_| std::sync::Mutex::new(None)).collect();
    let jobs = w.jobs.min(batch.len());
    std::thread::scope(|scope| {
        for _ in 0..jobs {
            let (next, out, batch) = (&next, &out, batch);
            let store = Arc::clone(store);
            scope.spawn(move || {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some(object) = batch.get(i) else { return };
                    let result = fetch(agent, w, object).and_then(|got| match got {
                        Fetched::Bytes(data, sha) => store
                            .put(&object.key(&w.tenant), &data)
                            .map_err(|e| format!("{}: writing to the store: {e}", object.path()))
                            .map(|_| Fetched::Bytes(data, sha)),
                        gone => Ok(gone),
                    });
                    *out[i].lock().expect("a fetch thread panicked") = Some(Outcome {
                        object: object.clone(),
                        result,
                    });
                }
            });
        }
    });
    out.into_iter()
        .filter_map(|slot| slot.into_inner().expect("a fetch thread panicked"))
        .collect()
}

pub async fn run(target: &mut Client, w: &Where) -> Result<Moved, String> {
    target
        .batch_execute(LEDGER)
        .await
        .map_err(|e| format!("the object ledger: {}", why(&e)))?;
    let rows = target
        .query(ROWS_SQL, &[])
        .await
        .map_err(|e| format!("reading storage.objects: {}", why(&e)))?;
    let all: Vec<Object> = rows
        .iter()
        .map(|r| Object {
            id: r.get(0),
            version: r.get(1),
            bucket: r.get(2),
            name: r.get(3),
            said: r.get(4),
        })
        .collect();
    let done: std::collections::BTreeSet<String> = target
        .query("select id from zou.import_objects", &[])
        .await
        .map_err(|e| format!("reading the object ledger: {}", why(&e)))?
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();

    let mut moved = Moved {
        manifest: w.manifest.clone(),
        ..Default::default()
    };
    let mut pending = Vec::new();
    for object in all {
        if done.contains(&object.id) {
            moved.already += 1;
        } else if object.version.is_empty() {
            // Without a version there is no key to write to. The
            // hosted platform sets one on every object, so this is a
            // row somebody made by hand, and it is named rather than
            // skipped in silence.
            moved
                .gone
                .push(format!("{}: the row has no version", object.path()));
        } else {
            pending.push(object);
        }
    }

    let store: Arc<dyn CasStore> =
        Arc::from(open_store(&w.store).map_err(|e| format!("store: {e}"))?);
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    let started = Instant::now();
    for batch in pending.chunks(CHUNK) {
        let outcomes = chunk(&agent, &store, w, batch);
        let mut written = Vec::new();
        for Outcome { object, result } in outcomes {
            match result? {
                Fetched::Bytes(data, sha) => {
                    if object.said >= 0 && object.said != data.len() as i64 {
                        moved.mismatched.push(format!(
                            "{}: the row says {} and the bytes are {}",
                            object.path(),
                            object.said,
                            data.len()
                        ));
                    }
                    moved.copied += 1;
                    moved.bytes += data.len() as u64;
                    written.push((object, data.len() as i64, sha));
                }
                Fetched::Gone(sentence) => {
                    moved.gone.push(format!("{}: {sentence}", object.path()));
                }
            }
        }
        record(target, &written).await?;
    }
    moved.seconds = started.elapsed().as_secs_f64();
    manifest(target, w).await?;
    Ok(moved)
}

/// The ledger rows for one finished chunk, in one transaction, so the
/// next run's idea of what is already there is either all of this
/// chunk or none of it.
async fn record(target: &mut Client, written: &[(Object, i64, String)]) -> Result<(), String> {
    if written.is_empty() {
        return Ok(());
    }
    let tx = target
        .transaction()
        .await
        .map_err(|e| format!("the object ledger: {}", why(&e)))?;
    let insert = tx
        .prepare(
            "insert into zou.import_objects (id, version, bucket, name, bytes, sha256) \
             values ($1, $2, $3, $4, $5, $6) on conflict (id) do nothing",
        )
        .await
        .map_err(|e| format!("the object ledger: {}", why(&e)))?;
    for (o, bytes, sha) in written {
        tx.execute(
            &insert,
            &[&o.id, &o.version, &o.bucket, &o.name, bytes, sha],
        )
        .await
        .map_err(|e| format!("the object ledger for {}: {}", o.path(), why(&e)))?;
    }
    tx.commit()
        .await
        .map_err(|e| format!("the object ledger: {}", why(&e)))
}

/// The integrity manifest, read back out of the ledger so that a run
/// which fetched nothing because a previous run fetched everything
/// still writes a complete one.
///
/// One line per object, digest first, in the shape `sha256sum` prints,
/// because that is the format somebody already has a tool for.
async fn manifest(target: &Client, w: &Where) -> Result<(), String> {
    let rows = target
        .query(
            "select sha256, bytes, bucket, name from zou.import_objects order by bucket, name",
            &[],
        )
        .await
        .map_err(|e| format!("reading the object ledger: {}", why(&e)))?;
    let mut out = String::from(
        "# sha256 of every storage object this import wrote, digest, size in bytes, then bucket and name\n",
    );
    for r in &rows {
        out.push_str(&format!(
            "{}  {}  {}/{}\n",
            r.get::<_, String>(0),
            r.get::<_, i64>(1),
            r.get::<_, String>(2),
            r.get::<_, String>(3)
        ));
    }
    std::fs::write(&w.manifest, out)
        .map_err(|e| format!("cannot write {}: {e}", w.manifest.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(bucket: &str, name: &str) -> Object {
        Object {
            id: "0d0f1a2b-3c4d-5e6f-7081-92a3b4c5d6e7".into(),
            version: "v1".into(),
            bucket: bucket.into(),
            name: name.into(),
            said: -1,
        }
    }

    /// The key a storage object's bytes go to is the server's own
    /// spelling with the tenant prefix in front. The server crate is
    /// only a dependency on unix, so the assertion that the two agree
    /// lives here and runs where it can.
    #[test]
    #[cfg(unix)]
    fn the_key_is_the_one_the_server_reads_from() {
        let o = object("avatars", "me.png");
        assert_eq!(
            o.key("local"),
            format!(
                "tenants/local/files/{}",
                zou_server::blob::key(&o.id, &o.version)
            )
        );
        assert_eq!(DEFAULT_TENANT, zou_server::blob::LOCAL);
    }

    #[test]
    fn the_key_carries_the_tenant_and_the_version() {
        let o = object("avatars", "me.png");
        assert_eq!(
            o.key("acme"),
            "tenants/acme/files/objects/0d0f1a2b-3c4d-5e6f-7081-92a3b4c5d6e7/v1"
        );
    }

    /// A name is a path on both sides, so its slashes stay and
    /// everything a url reads as structure goes.
    #[test]
    fn a_name_with_url_syntax_in_it_survives() {
        assert_eq!(
            url_for(
                "https://x.supabase.co/storage/v1/",
                &object("pics", "a b/c?d.png")
            ),
            "https://x.supabase.co/storage/v1/object/authenticated/pics/a%20b/c%3Fd.png"
        );
        assert_eq!(
            url_for(
                "https://x.supabase.co/storage/v1",
                &object("my bucket", "x#1.png")
            ),
            "https://x.supabase.co/storage/v1/object/authenticated/my%20bucket/x%231.png"
        );
    }

    #[test]
    fn the_hosted_url_is_the_one_the_dashboard_prints() {
        assert_eq!(
            base_for("abcdefghijklmnop"),
            "https://abcdefghijklmnop.supabase.co/storage/v1"
        );
    }

    #[test]
    fn the_digest_is_the_one_sha256sum_prints() {
        assert_eq!(
            digest(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    /// A run says what it copied and, every time, what it could not.
    #[test]
    fn a_run_names_every_object_it_did_not_bring() {
        let moved = Moved {
            copied: 2,
            already: 1,
            bytes: 3_000,
            gone: vec!["avatars/gone.png: the storage api answered 404".into()],
            mismatched: vec!["pics/a.png: the row says 10 and the bytes are 11".into()],
            manifest: PathBuf::from("import-objects.sha256"),
            seconds: 1.25,
        };
        let out = moved.render();
        assert!(
            out.starts_with("2 object bytes copied, 1 already there, 3.0 kB in 1.2s\n"),
            "{out}"
        );
        assert!(out.contains("no bytes for avatars/gone.png"), "{out}");
        assert!(out.contains("size disagrees for pics/a.png"), "{out}");
        assert!(
            out.contains("manifest written to import-objects.sha256"),
            "{out}"
        );
    }

    #[test]
    fn only_the_transient_statuses_are_tried_again() {
        for status in [429, 500, 502, 503, 504] {
            assert!(retryable(status), "{status}");
        }
        for status in [200, 401, 403, 404, 400, 501] {
            assert!(!retryable(status), "{status}");
        }
    }

    /// A storage api of its own, so the fetch, the retry, the 404 and
    /// the writes into the store are exercised without a hosted
    /// project or a network.
    struct FakeStorage {
        pub port: u16,
        pub calls: Arc<AtomicUsize>,
    }

    impl FakeStorage {
        /// Serves `avatars/me.png` and `pics/deep/one.txt`, answers 404
        /// for `avatars/gone.png`, and answers 503 once for
        /// `pics/flaky.txt` before answering it properly, which is the
        /// retry the network asks for.
        fn start() -> Self {
            use std::io::{BufRead, BufReader, Write};
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
            let port = listener.local_addr().expect("addr").port();
            let calls = Arc::new(AtomicUsize::new(0));
            let counter = Arc::clone(&calls);
            let flaked = Arc::new(AtomicUsize::new(0));
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else { return };
                    let counter = Arc::clone(&counter);
                    let flaked = Arc::clone(&flaked);
                    std::thread::spawn(move || {
                        let mut reader = BufReader::new(stream.try_clone().expect("clone"));
                        let mut line = String::new();
                        if reader.read_line(&mut line).is_err() {
                            return;
                        }
                        let mut authorized = false;
                        loop {
                            let mut header = String::new();
                            if reader.read_line(&mut header).unwrap_or(0) == 0 {
                                break;
                            }
                            if header
                                .to_ascii_lowercase()
                                .starts_with("authorization: bearer ")
                            {
                                authorized = true;
                            }
                            if header == "\r\n" || header == "\n" {
                                break;
                            }
                        }
                        counter.fetch_add(1, Ordering::Relaxed);
                        let path = line.split(' ').nth(1).unwrap_or("").to_string();
                        let body: &[u8] = match path.as_str() {
                            _ if !authorized => b"",
                            "/storage/v1/object/authenticated/avatars/me.png" => b"a picture",
                            "/storage/v1/object/authenticated/pics/deep/one.txt" => b"one",
                            "/storage/v1/object/authenticated/pics/flaky.txt" => b"flaky",
                            _ => b"",
                        };
                        let status = if !authorized {
                            "401 Unauthorized"
                        } else if path.ends_with("flaky.txt")
                            && flaked.fetch_add(1, Ordering::Relaxed) == 0
                        {
                            "503 Service Unavailable"
                        } else if body.is_empty() {
                            "404 Not Found"
                        } else {
                            "200 OK"
                        };
                        let head = format!(
                            "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ = stream.write_all(head.as_bytes());
                        let _ = stream.write_all(body);
                        let _ = stream.flush();
                    });
                }
            });
            Self { port, calls }
        }

        fn base(&self) -> String {
            format!("http://127.0.0.1:{}/storage/v1", self.port)
        }
    }

    /// Enough of a target to have object rows in it. `id` is uuid and
    /// `version` is text, which is what this server's own storage
    /// schema says.
    const SEED: &str = "
create schema if not exists storage;
create table storage.objects (
    id uuid primary key default gen_random_uuid(),
    bucket_id text,
    name text,
    metadata jsonb,
    version text
);
insert into storage.objects (bucket_id, name, metadata, version) values
    ('avatars', 'me.png', '{\"size\": 9}', 'v1'),
    ('avatars', 'gone.png', '{\"size\": 4}', 'v1'),
    ('pics', 'deep/one.txt', '{\"size\": 3}', 'v2'),
    ('pics', 'flaky.txt', '{\"size\": 99}', 'v3'),
    ('pics', 'handmade.txt', '{\"size\": 1}', null);
";

    /// The whole step against a real database and a real socket: the
    /// bytes land under the key the server reads them from, the object
    /// whose bytes are gone is named rather than fatal, the flaky one
    /// is fetched on the second try, the row whose size disagrees is
    /// reported and copied anyway, the manifest covers everything, and
    /// a second run downloads nothing.
    #[test]
    fn the_bytes_move_and_moving_them_twice_downloads_nothing() {
        let Ok(dsn) = std::env::var("ZOU_PG_TEST_DSN") else {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            return;
        };
        if dsn.is_empty() {
            eprintln!("skipping: ZOU_PG_TEST_DSN is empty");
            return;
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let base: tokio_postgres::Config = dsn.parse().expect("the dsn parses");
            let admin = crate::import::tests::open(&base).await;
            admin
                .batch_execute("drop database if exists zou_import_objects with (force)")
                .await
                .expect("drop");
            admin
                .batch_execute("create database zou_import_objects")
                .await
                .expect("create");
            let mut theirs = base.clone();
            theirs.dbname("zou_import_objects");
            let mut target = crate::import::tests::open(&theirs).await;
            target.batch_execute(SEED).await.expect("seed");

            let api = FakeStorage::start();
            let dir = tempfile::tempdir().expect("tempdir");
            let manifest = dir.path().join("import-objects.sha256");
            let w = Where {
                store: dir.path().join("store").display().to_string(),
                tenant: DEFAULT_TENANT.into(),
                base: api.base(),
                key: "a-service-role-key".into(),
                jobs: 4,
                manifest: manifest.clone(),
            };

            let moved = run(&mut target, &w).await.expect("the object copy");
            assert_eq!(moved.copied, 3, "{:?}", moved);
            assert_eq!(moved.already, 0);
            assert_eq!(moved.bytes, 9 + 3 + 5);
            let gone = moved.gone.join("\n");
            assert!(gone.contains("avatars/gone.png"), "{gone}");
            assert!(gone.contains("pics/handmade.txt"), "{gone}");
            assert!(gone.contains("no version"), "{gone}");
            let bad = moved.mismatched.join("\n");
            assert!(
                bad.contains("pics/flaky.txt") && bad.contains("says 99 and the bytes are 5"),
                "{bad}"
            );

            // The bytes are under the key the server reads them from.
            let store = open_store(&w.store).expect("store");
            let id: String = target
                .query_one(
                    "select id::text from storage.objects where name = 'me.png'",
                    &[],
                )
                .await
                .expect("the row")
                .get(0);
            let key = format!("tenants/local/files/objects/{id}/v1");
            assert_eq!(
                store.get(&key).expect("get").expect("the bytes").0,
                b"a picture"
            );

            let written = std::fs::read_to_string(&manifest).expect("the manifest");
            assert!(
                written.contains(&format!("{}  9  avatars/me.png", digest(b"a picture"))),
                "{written}"
            );
            assert!(written.contains("  3  pics/deep/one.txt"), "{written}");
            assert!(!written.contains("gone.png"), "{written}");

            let before = api.calls.load(Ordering::Relaxed);
            let again = run(&mut target, &w).await.expect("the second object copy");
            assert_eq!(again.copied, 0);
            assert_eq!(again.already, 3, "the ledger carried the first run over");
            assert_eq!(
                api.calls.load(Ordering::Relaxed) - before,
                1,
                "only the row with no bytes is asked for again"
            );
            assert!(
                std::fs::read_to_string(&manifest)
                    .expect("the manifest")
                    .contains("avatars/me.png"),
                "a resumed run still writes the whole manifest"
            );

            drop(target);
            admin
                .batch_execute("drop database zou_import_objects with (force)")
                .await
                .expect("drop");
        });
    }
}
