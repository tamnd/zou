//! A test suite's shape: one seeded database, a branch of it per test.
//!
//! A fixture is empty, so a suite that opens one per test runs its
//! migrations per test as well, and the migrations are usually the
//! expensive half. This applies them once and gives every test a copy
//! on write branch of the result, which is the same trick fixtures play
//! on the machine's template, one level further down.
//!
//!   ZOU_PG_BIN=$PWD/build/pg/bin cargo run --release -p zou-embed \
//!       --example per_test_branch -- 10
//!
//! It prints what the seeding cost and what each test cost, and checks
//! the thing the pattern stands on: a test sees the seed, does not see
//! what any other test wrote, and takes its database off the store on
//! the way out.

#[cfg(unix)]
use std::path::PathBuf;
#[cfg(unix)]
use std::time::Instant;

#[cfg(unix)]
use zou_embed::{Options, Zou};

#[cfg(not(unix))]
fn main() {
    println!("the embedded library is unix only for now");
}

/// The migrations a real suite would run out of its own directory.
#[cfg(unix)]
const SCHEMA: &str = "create table public.todos (id serial primary key, title text not null);
     insert into public.todos (title) values ('write the migration'), ('run the suite');
     grant select, insert on public.todos to anon;
     grant usage on sequence public.todos_id_seq to anon;";

#[cfg(unix)]
fn main() {
    env_logger::init();
    let tests: usize = std::env::args()
        .skip(1)
        .find_map(|a| a.parse().ok())
        .unwrap_or(10);
    let pg_bin = match std::env::var("ZOU_PG_BIN") {
        Ok(v) if !v.is_empty() => PathBuf::from(v),
        _ => PathBuf::from("build/pg/bin"),
    };

    // Once for the whole suite: a fixture out of the machine's template
    // with this project's schema in it. Every test below is a branch of
    // this, so nothing runs the migrations again.
    let at = Instant::now();
    let base = Zou::open(Options {
        pg_bin,
        ..Options::fixture()
    })
    .expect("open the base fixture");
    sql(&base, SCHEMA);
    println!(
        "the base, template and schema included: {}",
        ms(at.elapsed())
    );

    let mut took = Vec::with_capacity(tests);
    for n in 0..tests {
        let at = Instant::now();
        let test = base
            .branch(&format!("test-{n}"))
            .expect("a branch per test");
        let opened = at.elapsed();

        // What the seed gave this test, and what this test does to it.
        let key = test.keys().anon.clone();
        let seen = test
            .request("GET", "/rest/v1/todos", &[("apikey", &key)], b"")
            .expect("request");
        assert_eq!(seen.status, 200, "{}", text(&seen));
        assert_eq!(rows(&seen), 2, "the seed is there and nothing else is");
        let wrote = test
            .request(
                "POST",
                "/rest/v1/todos",
                &[("apikey", &key), ("content-type", "application/json")],
                format!(r#"{{"title":"test {n} was here"}}"#).as_bytes(),
            )
            .expect("request");
        assert_eq!(wrote.status, 201, "{}", text(&wrote));

        took.push(opened.as_secs_f64() * 1000.0);
        test.close().expect("close");
    }

    // The suite is over and the store is back to holding the base, so a
    // suite run does not leave a database per test behind it.
    println!("{tests} tests, each a branch of the base, milliseconds:");
    took.sort_by(f64::total_cmp);
    for (name, at) in [("p50", 0.50), ("p90", 0.90)] {
        let i = ((took.len() as f64 * at).ceil() as usize).saturating_sub(1);
        println!("  {name}  {:.1}", took[i]);
    }
    println!("  min  {:.1}", took[0]);
    println!("  max  {:.1}", took[took.len() - 1]);

    // And the base still has exactly what it was seeded with, because
    // every test wrote into its own branch.
    let key = base.keys().anon.clone();
    let after = base
        .request("GET", "/rest/v1/todos", &[("apikey", &key)], b"")
        .expect("request");
    assert_eq!(
        rows(&after),
        2,
        "the base is untouched, {tests} tests wrote into their own"
    );
    println!("the base is still the two rows it was seeded with");
    base.close().expect("close the base");
}

/// The suite's migrations, run the way a host process runs them.
#[cfg(unix)]
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
        client.batch_execute(statements).await.expect("the schema");
        drop(client);
        let _ = pump.await;
    });
}

#[cfg(unix)]
fn text(answer: &zou_embed::Response) -> String {
    String::from_utf8_lossy(&answer.body).into_owned()
}

/// How many objects came back, counted the cheap way rather than by
/// pulling a json parser in for one number.
#[cfg(unix)]
fn rows(answer: &zou_embed::Response) -> usize {
    text(answer).matches("\"id\":").count()
}

#[cfg(unix)]
fn ms(took: std::time::Duration) -> String {
    format!("{:.1} ms", took.as_secs_f64() * 1000.0)
}
