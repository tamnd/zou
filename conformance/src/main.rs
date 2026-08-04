//! The compatibility harness: one suite, several targets, and the
//! differences between their answers.
//!
//! Compatibility is not a thing you can assert about yourself. Either
//! the answer matches what the thing being copied says, or the claim is
//! marketing. So a suite is questions, not expectations, and the
//! expectations are recordings of what PostgREST and GoTrue actually
//! answered, at the versions pinned in versions.json.
//!
//! The suites themselves are not in this repository. They are upstream's
//! fixtures and upstream's answers, megabytes of them, and they change
//! when upstream changes rather than when zou does, so they live in
//! tamnd/zou-conformance and this reads a checkout of it.
//!
//! Three modes, and the difference between them is only where the other
//! answer comes from.
//!
//!   record  ask a reference and write down what it said
//!   check   ask a target and compare with what was written down
//!   diff    ask two targets at once and compare them live
//!
//! `check` is what CI runs, because it needs no reference on the
//! machine and it fails on the day upstream and zou drift apart rather
//! than on the day somebody remembers to look. `diff` is what you run
//! when you have both up and you do not trust the recording.
//!
//! A fourth, `derive`, asks nothing. It reads a PostgREST checkout and
//! writes a suite out of its spec files, which is how the second suite
//! here got written.
//!
//! And a fifth, `serve`, answers rather than asks. It starts zou on a
//! port and waits, so that a suite written in another language, against
//! a client somebody else wrote, has a url to point at.
//!
//! A sixth, `scoreboard`, asks nothing and serves nothing. It turns the
//! json a run wrote into the markdown CI commits, so that the numbers
//! are in the repository rather than in a log that expires.

mod derive;
mod diff;
mod report;
mod scoreboard;
mod suite;
mod target;
mod zou;

use std::process::ExitCode;

use diff::compare;
use report::{Report, Result as CaseResult};
use suite::Suite;
use target::Target;

const USAGE: &str = "\
usage: zou-conformance <mode> --suite <name> [options]

modes
  record    ask a target and write <suites>/<name>/recorded.json
  check     ask a target and compare it with that recording
  diff      ask two targets and compare them with each other
  derive    read a PostgREST checkout and write a suite out of it, no target
  serve     start zou on a port and wait, for a suite that is not asked
            from here: the supabase-js one, or a browser
  scoreboard  turn the json those runs wrote into markdown, no target

the scoreboard
  --report <path>          a report json a check wrote with --json, repeatable
  --js <path>              vitest's json from the supabase-js run
  --pin <sha>              the zou-conformance commit the suites came from
  --out <path>             where to write it, stdout when absent

serving
  --zou-dsn <dsn>          the database it reads
  --port <n>               where it answers, default 54321, the port the
                           supabase CLI serves a local project on
  --schemas <a,b>          what a request that names no schema gets,
                           default public
  --setup <path>           a sql file to apply once the server is up, which
                           is where the fixture of a suite asked elsewhere
                           goes. Applied after the auth schema exists, so it
                           may reference auth.users

deriving
  --from <path>            a PostgREST source tree at the pinned version
  --suite <name>           the suite to write, which is created if it is new

the target
  --url <url>              where it answers, no trailing slash needed
  --dsn <dsn>              the database behind it, so setup.sql can be applied
  --anon <jwt>             the anon key, minted from --jwt-secret when absent
  --authenticated <jwt>    a key carrying the authenticated role, likewise
  --service <jwt>          the service_role key, likewise
  --name <name>            what the report calls it, default the url
  --zou-dsn <dsn>          instead of --url: start zou here, on a free port
  --strip-prefix <path>    a prefix this target does not serve under, which
                           is /rest/v1 for a bare PostgREST

the other one, for diff
  --reference-url <url>    and --reference-dsn, --reference-anon,
                           --reference-service, --reference-name,
                           --reference-strip-prefix

everything else
  --suites <dir>           a checkout of tamnd/zou-conformance, the suites
                           directory in it, also read from the environment
                           as ZOU_CONFORMANCE_SUITES
  --suite <name>           which suite, or all of them when repeated or absent
  --jwt-secret <secret>    the hs256 secret both ends sign with
  --json <path>            write the report as json as well
  --no-setup               do not apply setup.sql, the database is ready
  --write-known            write the run's differences to known.json rather
                           than failing over them, which is how a suite with
                           hundreds of them gets a ratchet. Read the diff.";

/// The secret supabase's own local stack ships with. Not a default
/// worth hiding: every target in a conformance run has to sign with the
/// same one or the keys are not the same keys.
const DEMO_SECRET: &str = "super-secret-jwt-token-with-at-least-32-characters-long";

struct Args {
    mode: String,
    suites: Vec<String>,
    url: Option<String>,
    dsn: Option<String>,
    anon: Option<String>,
    authenticated: Option<String>,
    service: Option<String>,
    name: Option<String>,
    zou_dsn: Option<String>,
    strip: Option<String>,
    reference_url: Option<String>,
    reference_dsn: Option<String>,
    reference_anon: Option<String>,
    reference_service: Option<String>,
    reference_name: Option<String>,
    reference_strip: Option<String>,
    jwt_secret: String,
    json: Option<String>,
    setup: bool,
    from: Option<String>,
    write_known: bool,
    suites_dir: Option<String>,
    port: u16,
    schemas: Vec<String>,
    setup_sql: Option<String>,
    reports: Vec<String>,
    js: Option<String>,
    pin: Option<String>,
    out: Option<String>,
}

fn parse(argv: &[String]) -> Result<Args, String> {
    let mut args = Args {
        mode: String::new(),
        suites: Vec::new(),
        url: None,
        dsn: None,
        anon: None,
        authenticated: None,
        service: None,
        name: None,
        zou_dsn: None,
        strip: None,
        reference_url: None,
        reference_dsn: None,
        reference_anon: None,
        reference_service: None,
        reference_name: None,
        reference_strip: None,
        jwt_secret: DEMO_SECRET.to_string(),
        json: None,
        setup: true,
        from: None,
        write_known: false,
        suites_dir: None,
        port: 54321,
        schemas: vec!["public".to_string()],
        setup_sql: None,
        reports: Vec::new(),
        js: None,
        pin: None,
        out: None,
    };
    let mut it = argv.iter();
    args.mode = match it.next() {
        Some(mode) => mode.clone(),
        None => return Err("no mode".to_string()),
    };
    if !matches!(
        args.mode.as_str(),
        "record" | "check" | "diff" | "derive" | "serve" | "scoreboard"
    ) {
        return Err(format!("no mode named {:?}", args.mode));
    }
    while let Some(arg) = it.next() {
        let mut need = |flag: &str| -> Result<String, String> {
            it.next()
                .cloned()
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        match arg.as_str() {
            "--suite" => args.suites.push(need("--suite")?),
            "--url" => args.url = Some(need("--url")?),
            "--dsn" => args.dsn = Some(need("--dsn")?),
            "--anon" => args.anon = Some(need("--anon")?),
            "--authenticated" => args.authenticated = Some(need("--authenticated")?),
            "--service" => args.service = Some(need("--service")?),
            "--name" => args.name = Some(need("--name")?),
            "--zou-dsn" => args.zou_dsn = Some(need("--zou-dsn")?),
            "--strip-prefix" => args.strip = Some(need("--strip-prefix")?),
            "--reference-url" => args.reference_url = Some(need("--reference-url")?),
            "--reference-dsn" => args.reference_dsn = Some(need("--reference-dsn")?),
            "--reference-anon" => args.reference_anon = Some(need("--reference-anon")?),
            "--reference-service" => args.reference_service = Some(need("--reference-service")?),
            "--reference-name" => args.reference_name = Some(need("--reference-name")?),
            "--reference-strip-prefix" => {
                args.reference_strip = Some(need("--reference-strip-prefix")?)
            }
            "--jwt-secret" => args.jwt_secret = need("--jwt-secret")?,
            "--json" => args.json = Some(need("--json")?),
            "--no-setup" => args.setup = false,
            "--from" => args.from = Some(need("--from")?),
            "--write-known" => args.write_known = true,
            "--suites" => args.suites_dir = Some(need("--suites")?),
            "--setup" => args.setup_sql = Some(need("--setup")?),
            "--report" => args.reports.push(need("--report")?),
            "--js" => args.js = Some(need("--js")?),
            "--pin" => args.pin = Some(need("--pin")?),
            "--out" => args.out = Some(need("--out")?),
            "--port" => {
                let value = need("--port")?;
                args.port = value
                    .parse()
                    .map_err(|_| format!("--port takes a number, not {value:?}"))?;
            }
            "--schemas" => {
                args.schemas = need("--schemas")?
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
            other => return Err(format!("no flag named {other:?}")),
        }
    }
    // The scoreboard reads what a run already wrote. It asks nothing,
    // needs no database, and cannot be run against a target, which is
    // the point: the numbers on it came out of a run somebody can point
    // at rather than out of the command that rendered them.
    if args.mode == "scoreboard" {
        if args.reports.is_empty() {
            return Err("scoreboard needs a --report to read".to_string());
        }
        if args.url.is_some() || args.zou_dsn.is_some() {
            return Err("scoreboard reads a report, so it takes no target".to_string());
        }
        return Ok(args);
    }
    if !args.reports.is_empty() || args.js.is_some() || args.pin.is_some() || args.out.is_some() {
        return Err(format!(
            "--report, --js, --pin and --out are for scoreboard, not {}",
            args.mode
        ));
    }
    // Serving asks nothing and reads no suite. It is zou on a port and
    // a process that stays up, so the only thing it needs is a database.
    if args.mode == "serve" {
        if args.zou_dsn.is_none() {
            return Err("serve needs a --zou-dsn to read".to_string());
        }
        if args.url.is_some() || args.reference_url.is_some() {
            return Err("serve is the target, so it takes no other one".to_string());
        }
        if args.schemas.is_empty() {
            return Err("--schemas needs at least one".to_string());
        }
        return Ok(args);
    }
    if args.setup_sql.is_some() {
        return Err("--setup is for serve, the other modes apply the suite's own".to_string());
    }
    // Deriving reads a checkout and writes files. There is nothing to
    // ask, so a target would be a command somebody meant differently.
    if args.mode == "derive" {
        if args.from.is_none() {
            return Err("derive needs a --from pointing at a PostgREST checkout".to_string());
        }
        if args.suites.len() != 1 {
            return Err("derive writes one suite, name it with --suite".to_string());
        }
        if args.url.is_some() || args.zou_dsn.is_some() {
            return Err("derive asks nothing, so it takes no target".to_string());
        }
        return Ok(args);
    }
    if args.from.is_some() {
        return Err(format!(
            "{} reads a suite rather than deriving one",
            args.mode
        ));
    }
    if args.url.is_none() && args.zou_dsn.is_none() {
        return Err("no target: pass --url or --zou-dsn".to_string());
    }
    if args.url.is_some() && args.zou_dsn.is_some() {
        return Err("--url and --zou-dsn are two targets, pass one".to_string());
    }
    if args.mode == "diff" && args.reference_url.is_none() {
        return Err("diff needs a --reference-url to diff against".to_string());
    }
    if args.write_known && args.mode != "check" {
        return Err("--write-known writes down what a check found, so it needs check".to_string());
    }
    if args.mode != "diff" && args.reference_url.is_some() {
        return Err(format!("{} takes one target, not a reference", args.mode));
    }
    Ok(args)
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let args = match parse(&argv) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("zou-conformance: {message}");
            eprintln!("{USAGE}");
            return ExitCode::FAILURE;
        }
    };
    if let Some(dir) = &args.suites_dir {
        suite::use_suites(std::path::PathBuf::from(dir));
    }
    match run(args) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(message) => {
            eprintln!("zou-conformance: {message}");
            ExitCode::FAILURE
        }
    }
}

/// True when everything matched.
fn run(args: Args) -> Result<bool, String> {
    if args.mode == "derive" {
        return written(&args);
    }
    if args.mode == "serve" {
        return holding(&args);
    }
    if args.mode == "scoreboard" {
        return published(&args);
    }
    let names = match args.suites.is_empty() {
        true => Suite::all()?,
        false => args.suites.clone(),
    };
    let mut reports = Vec::new();
    for suite in &names {
        let suite = Suite::load(suite)?;
        // Per suite rather than once, because a suite says which
        // schemas it needs and which role its anon key carries, and two
        // suites do not have to agree about either.
        let anon = args
            .anon
            .clone()
            .unwrap_or_else(|| zou::key(&suite.cases.anon_role, &args.jwt_secret));
        let authenticated = args
            .authenticated
            .clone()
            .unwrap_or_else(|| zou::key("authenticated", &args.jwt_secret));
        let service = args
            .service
            .clone()
            .unwrap_or_else(|| zou::key("service_role", &args.jwt_secret));
        // Started before anything is asked so that a target that cannot
        // come up is one message rather than a suite's worth of them.
        let served = match &args.zou_dsn {
            Some(dsn) => Some(zou::start(
                dsn,
                args.jwt_secret.as_bytes(),
                &suite.cases.schemas,
            )?),
            None => None,
        };
        let (url, dsn) = match &served {
            Some(served) => (served.url.clone(), args.zou_dsn.clone()),
            None => (args.url.clone().unwrap_or_default(), args.dsn.clone()),
        };
        let name = args.name.clone().unwrap_or_else(|| match &served {
            Some(_) => "zou".to_string(),
            None => url.clone(),
        });
        let target = Target::new(
            &name,
            &url,
            Some(anon.clone()),
            Some(authenticated.clone()),
            Some(service.clone()),
            dsn,
            args.strip.clone(),
        );
        let reference = args.reference_url.as_ref().map(|url| {
            let name = args.reference_name.clone().unwrap_or_else(|| url.clone());
            Target::new(
                &name,
                url,
                Some(args.reference_anon.clone().unwrap_or_else(|| anon.clone())),
                Some(authenticated.clone()),
                Some(
                    args.reference_service
                        .clone()
                        .unwrap_or_else(|| service.clone()),
                ),
                args.reference_dsn.clone(),
                args.reference_strip.clone(),
            )
        });
        let setup = match args.setup {
            true => Some(suite.setup.as_str()),
            false => None,
        };
        match args.mode.as_str() {
            "record" => {
                let recording = record(&suite, &target, setup)?;
                let path = suite.recording_path();
                let text = serde_json::to_string_pretty(&recording)
                    .map_err(|e| format!("writing the recording: {e}"))?;
                std::fs::write(&path, format!("{text}\n"))
                    .map_err(|e| format!("{}: {e}", path.display()))?;
                println!(
                    "recorded {} answers from {} into {}",
                    recording.answers.len(),
                    target.name,
                    path.display()
                );
            }
            "check" => {
                let report = check(&suite, &target, setup)?;
                if args.write_known {
                    write_known(&suite, &report)?;
                }
                reports.push(report);
            }
            "diff" => {
                let reference = reference.as_ref().expect("parse checked for one");
                reports.push(against(&suite, &target, reference, setup)?);
            }
            _ => unreachable!("parse checked the mode"),
        }
    }

    let mut good = true;
    for report in &reports {
        print!("{}", report.text());
        good &= !report.failed();
    }
    if let Some(path) = &args.json {
        let json = serde_json::json!({
            "suites": reports.iter().map(Report::json).collect::<Vec<_>>(),
        });
        let text =
            serde_json::to_string_pretty(&json).map_err(|e| format!("writing the report: {e}"))?;
        std::fs::write(path, format!("{text}\n")).map_err(|e| format!("{path}: {e}"))?;
    }
    Ok(good)
}

/// The scoreboard: the json a run wrote, as the markdown CI commits.
///
/// It writes over the file rather than appending to it, and it puts no
/// date in it, so the diff of a merge is the numbers that moved and
/// nothing else. A merge that changes no number changes no file, and
/// then there is no commit at all.
fn published(args: &Args) -> Result<bool, String> {
    let runs = scoreboard::read(&args.reports)?;
    let js = match &args.js {
        Some(path) => Some(scoreboard::read_js(path)?),
        None => None,
    };
    let text = scoreboard::render(&runs, js.as_ref(), args.pin.as_deref());
    match &args.out {
        Some(path) => {
            std::fs::write(path, &text).map_err(|e| format!("{path}: {e}"))?;
            println!("wrote {path}, {} suites", runs.len());
        }
        None => print!("{text}"),
    }
    Ok(true)
}

/// Serving: zou on a known port, and then nothing.
///
/// Everything else here asks the questions itself. This exists for the
/// suites that cannot be asked from here, the supabase-js one first,
/// where the client is somebody else's library in somebody else's
/// language and the only thing it takes is a url and a key. The keys
/// are printed because they are minted from the secret and a shell
/// script should not have to know how.
///
/// It never returns. The process is killed by whatever started it,
/// which is how a server in a CI step is always ended.
///
/// `--setup` is applied after the server is up rather than before,
/// because zou installs the auth schema on its first connection and a
/// fixture with a foreign key into auth.users would otherwise be a race
/// against the server's own bootstrap. start_at does not come back until
/// that has happened.
fn holding(args: &Args) -> Result<bool, String> {
    let dsn = args.zou_dsn.as_deref().expect("parse checked for one");
    let served = zou::start_at(args.port, dsn, args.jwt_secret.as_bytes(), &args.schemas)?;
    if let Some(path) = &args.setup_sql {
        let sql = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
        target::apply(dsn, &sql, "serve")?;
        println!("setup {path}");
    }
    println!("url {}", served.url);
    for role in ["anon", "authenticated", "service_role"] {
        println!("{role} {}", zou::key(role, &args.jwt_secret));
    }
    println!("schemas {}", args.schemas.join(","));
    loop {
        std::thread::park();
    }
}

/// Deriving: a checkout in, a suite on disk out.
///
/// The recording is deliberately not written or touched. A derived
/// suite is a set of questions, and the answers still have to come from
/// asking the reference, which is `record` and a running PostgREST.
fn written(args: &Args) -> Result<bool, String> {
    let from = std::path::PathBuf::from(args.from.clone().expect("parse checked for one"));
    let name = args.suites.first().expect("parse checked for one");
    let derived = derive::derive(&from, name)?;
    let dir = suite::suites_dir().join(name);
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    write(&dir.join("setup.sql"), &derived.setup)?;
    write(&dir.join("reset.sql"), &derived.reset)?;
    let cases = serde_json::to_string_pretty(&derived.cases)
        .map_err(|e| format!("writing the cases: {e}"))?;
    write(&dir.join("cases.json"), &format!("{cases}\n"))?;
    for skipped in &derived.skipped {
        println!("skipped {skipped}");
    }
    println!(
        "derived {} cases into {}, {} requests not understood",
        derived.cases.cases.len(),
        dir.display(),
        derived.skipped.len()
    );
    Ok(true)
}

/// The run's differences, written down as the list they are excused by.
///
/// Every entry says what actually differs, taken from the report, so
/// the file reads as the list of things zou does not do yet rather than
/// as a list of names. It is checked in, which is the whole point: the
/// next run fails on anything that is not in it, and on anything in it
/// that has started to agree.
fn write_known(suite: &Suite, report: &Report) -> Result<(), String> {
    let known: Vec<serde_json::Value> = report
        .results
        .iter()
        .filter(|r| r.difference.verdict == diff::Verdict::Different)
        .map(|r| {
            serde_json::json!({
                "name": r.name,
                "why": r.difference.lines.first().cloned().unwrap_or_default(),
            })
        })
        .collect();
    let path = suite.known_path();
    let text =
        serde_json::to_string_pretty(&known).map_err(|e| format!("writing the known list: {e}"))?;
    write(&path, &format!("{text}\n"))?;
    println!("wrote {} differences to {}", known.len(), path.display());
    Ok(())
}

fn write(path: &std::path::Path, text: &str) -> Result<(), String> {
    std::fs::write(path, text).map_err(|e| format!("{}: {e}", path.display()))
}

/// Statistics that hold still for the length of a run.
///
/// The order of the rows inside an embed is not something either server
/// promises. It falls out of the plan, and the plan changes the moment
/// the planner learns how many rows a fixture table holds: with no
/// statistics postgres hashes one side of the join, with statistics it
/// hashes the other, and the same question gets the same rows back in a
/// different order. A run is twelve hundred cases and several minutes
/// long, autovacuum wakes up every minute, and the writing cases put
/// the rows back three hundred and sixty seven times, so somewhere in
/// the middle of a run autovacuum analyzes the fixtures and every case
/// after that point is answered off a different plan than every case
/// before it. Which cases those are depends on how fast the machine is,
/// which is how a recording made on one machine stops reproducing on
/// another and how a change to something unrelated moves five cases
/// across the line.
///
/// So the fixtures are told not to be analyzed, once, immediately after
/// they are created and before anything is asked. Nothing here needs a
/// good plan. It needs the same plan on the first case as on the last,
/// and the same plan in CI as on a laptop.
///
/// Only ordinary tables, since a partitioned parent is never autoanalyzed
/// and a materialized view is only analyzed when it is refreshed. Each
/// one in its own block, so that a database somebody else owns loses the
/// tables it can and keeps going rather than failing the run.
const STILL: &str = "\
do $$
declare
  relation regclass;
begin
  for relation in
    select c.oid::regclass
      from pg_class c
      join pg_namespace n on n.oid = c.relnamespace
     where c.relkind = 'r'
       and n.nspname not in ('pg_catalog', 'information_schema')
  loop
    begin
      execute format('alter table %s set (autovacuum_enabled = off)', relation);
    exception when insufficient_privilege then
      null;
    end;
  end loop;
end
$$;
";

/// The suite's schema, and then every case in order.
///
/// The setup runs immediately before the questions rather than once for
/// everybody, because the writing cases leave rows behind and the next
/// target has to see the same database the last one started with. It is
/// also why two targets are asked one after the other rather than side
/// by side: they are usually reading the same postgres.
fn ask(target: &Target, suite: &Suite, setup: Option<&str>) -> Result<Asked, String> {
    target.utc()?;
    if let Some(setup) = setup {
        target.set_up(setup)?;
        target.set_up(STILL)?;
        // A target that keeps its own picture of the schema has just
        // been told the schema moved, and it is told over a
        // notification rather than over the connection the setup ran
        // on, so there is nothing to wait on but the clock.
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    let mut asked = Asked {
        answers: Vec::new(),
        errors: Vec::new(),
    };
    // Upstream's own suite gets this for free by rolling every
    // transaction back. Here the rows go back the hard way, before any
    // case that is going to change them, so that a case is asked
    // against the rows it was written against and not against what the
    // twenty cases before it left behind.
    let reset = match setup.is_some() {
        true => suite.reset.as_deref(),
        false => None,
    };
    for case in &suite.cases.cases {
        if case.writes
            && let Some(reset) = reset
        {
            target.set_up(reset)?;
        }
        match target.send(case) {
            Ok(answer) => asked.answers.push(answer),
            Err(message) => asked.errors.push((case.name.clone(), message)),
        }
    }
    Ok(asked)
}

struct Asked {
    answers: Vec<suite::Answer>,
    errors: Vec<(String, String)>,
}

fn record(suite: &Suite, target: &Target, setup: Option<&str>) -> Result<suite::Recording, String> {
    let asked = ask(target, suite, setup)?;
    if let Some((name, message)) = asked.errors.first() {
        // A recording with a hole in it is worse than no recording,
        // since the hole is what every later run is compared against.
        return Err(format!("{name}: {message}"));
    }
    Ok(suite::Recording {
        suite: suite.name.clone(),
        recorded_from: target.name.clone(),
        answers: asked.answers,
    })
}

fn check(suite: &Suite, target: &Target, setup: Option<&str>) -> Result<Report, String> {
    let recording = suite.recording()?;
    let asked = ask(target, suite, setup)?;
    let mut report = Report {
        suite: suite.name.clone(),
        against: target.name.clone(),
        compared_with: format!("{} (recorded)", recording.recorded_from),
        results: Vec::new(),
        errors: asked.errors,
        known: suite.known(),
    };
    fill(&mut report, suite, &recording.answers, &asked.answers);
    Ok(report)
}

/// One result per case that both sides answered, and an error for a
/// case only one of them did.
fn fill(report: &mut Report, suite: &Suite, expected: &[suite::Answer], found: &[suite::Answer]) {
    for case in &suite.cases.cases {
        let want = expected.iter().find(|a| a.name == case.name);
        let got = found.iter().find(|a| a.name == case.name);
        match (want, got) {
            (Some(want), Some(got)) => report.results.push(CaseResult {
                name: case.name.clone(),
                feature: case.feature.clone(),
                method: case.method.clone(),
                path: case.path.clone(),
                difference: compare(want, got),
            }),
            (None, Some(_)) => report
                .errors
                .push((case.name.clone(), "nothing recorded for it".to_string())),
            // Already an error of its own, from whichever side did not
            // answer it.
            (_, None) => {}
        }
    }
}

fn against(
    suite: &Suite,
    target: &Target,
    reference: &Target,
    setup: Option<&str>,
) -> Result<Report, String> {
    // The reference goes first and finishes, so that a case the
    // reference itself cannot answer is a broken case rather than zou
    // being wrong about it, and so that the writing cases meet the
    // same rows twice.
    let expected = ask(reference, suite, setup)?;
    let found = ask(target, suite, setup)?;
    let mut errors = expected.errors;
    errors.extend(found.errors);
    let mut report = Report {
        suite: suite.name.clone(),
        against: target.name.clone(),
        compared_with: reference.name.clone(),
        results: Vec::new(),
        errors,
        known: suite.known(),
    };
    fill(&mut report, suite, &expected.answers, &found.answers);
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|a| a.to_string()).collect()
    }

    #[test]
    fn a_mode_and_a_target_is_the_whole_of_it() {
        let args = parse(&argv(&["check", "--url", "http://127.0.0.1:3000"])).expect("parses");
        assert_eq!(args.mode, "check");
        assert!(args.suites.is_empty(), "no suite means all of them");
        assert!(args.setup);
        assert_eq!(args.jwt_secret, DEMO_SECRET);
    }

    #[test]
    fn suites_are_named_one_flag_at_a_time() {
        let args = parse(&argv(&[
            "check", "--suite", "rest", "--suite", "auth", "--url", "u",
        ]))
        .expect("parses");
        assert_eq!(args.suites, ["rest", "auth"]);
    }

    #[test]
    fn a_run_without_a_target_is_an_error_and_not_a_default() {
        assert!(parse(&argv(&["check", "--suite", "rest"])).is_err());
    }

    #[test]
    fn two_targets_in_the_place_of_one_is_an_error() {
        assert!(parse(&argv(&["check", "--url", "u", "--zou-dsn", "d"])).is_err());
    }

    #[test]
    fn diff_says_what_it_is_diffing_against() {
        assert!(parse(&argv(&["diff", "--url", "u"])).is_err());
        assert!(parse(&argv(&["diff", "--url", "u", "--reference-url", "r"])).is_ok());
    }

    /// A reference in a mode that has nothing to do with one is a
    /// command somebody meant differently.
    #[test]
    fn a_reference_where_it_cannot_be_used_is_an_error() {
        assert!(parse(&argv(&["check", "--url", "u", "--reference-url", "r"])).is_err());
    }

    #[test]
    fn a_flag_with_nothing_after_it_is_an_error() {
        assert!(parse(&argv(&["check", "--url"])).is_err());
        assert!(parse(&argv(&["check", "--url", "u", "--suite"])).is_err());
    }

    #[test]
    fn a_mode_nobody_has_is_an_error() {
        assert!(parse(&argv(&["compare", "--url", "u"])).is_err());
        assert!(parse(&argv(&[])).is_err());
    }

    #[test]
    fn a_flag_nobody_has_is_an_error() {
        assert!(parse(&argv(&["check", "--url", "u", "--fast"])).is_err());
    }

    /// The port is the point of serve: the suite that uses it is asked
    /// by somebody else's client, and that client is handed a url.
    #[test]
    fn serve_answers_where_a_local_supabase_project_does() {
        let args = parse(&argv(&["serve", "--zou-dsn", "d"])).expect("parses");
        assert_eq!(args.port, 54321);
        assert_eq!(args.schemas, ["public"]);
        assert!(args.setup_sql.is_none());
        let args = parse(&argv(&["serve", "--zou-dsn", "d", "--port", "8000"])).expect("parses");
        assert_eq!(args.port, 8000);
    }

    #[test]
    fn serve_is_the_target_rather_than_asking_one() {
        assert!(parse(&argv(&["serve"])).is_err());
        assert!(parse(&argv(&["serve", "--zou-dsn", "d", "--url", "u"])).is_err());
        assert!(parse(&argv(&["serve", "--zou-dsn", "d", "--port", "eight"])).is_err());
    }

    /// A fixture applied by hand belongs to the suite that is asked
    /// elsewhere. The modes that read a suite already have its own
    /// setup.sql, so --setup there is a command somebody meant
    /// differently.
    #[test]
    fn a_setup_file_is_for_serving_and_nothing_else() {
        let args = parse(&argv(&[
            "serve",
            "--zou-dsn",
            "d",
            "--setup",
            "js/setup.sql",
        ]))
        .expect("parses");
        assert_eq!(args.setup_sql.as_deref(), Some("js/setup.sql"));
        assert!(parse(&argv(&["check", "--url", "u", "--setup", "js/setup.sql"])).is_err());
    }

    #[test]
    fn the_scoreboard_reads_reports_and_asks_nothing() {
        let args = parse(&argv(&[
            "scoreboard",
            "--report",
            "rest.json",
            "--report",
            "postgrest.json",
            "--out",
            "docs/scoreboard.md",
        ]))
        .expect("parses");
        assert_eq!(args.reports, ["rest.json", "postgrest.json"]);
        assert_eq!(args.out.as_deref(), Some("docs/scoreboard.md"));
        assert!(parse(&argv(&["scoreboard", "--out", "docs/scoreboard.md"])).is_err());
        assert!(parse(&argv(&["scoreboard", "--report", "r.json", "--url", "u"])).is_err());
    }

    /// A scoreboard rendered from a run that is happening right now is
    /// a scoreboard nobody can point at afterwards, so the flags for it
    /// do not exist in the modes that ask.
    #[test]
    fn a_report_is_not_something_a_run_takes() {
        assert!(parse(&argv(&["check", "--url", "u", "--report", "r.json"])).is_err());
        assert!(parse(&argv(&["serve", "--zou-dsn", "d", "--pin", "abc"])).is_err());
    }
}
