mod branch;
mod codegen;
mod compact;
#[cfg(unix)]
mod dev;
#[cfg(unix)]
mod inbox;
mod info;
mod inspect;
mod map;
#[cfg(unix)]
mod serve;
mod shard;
mod stats;
mod sync;
mod tenant;

use std::process::ExitCode;

/// The postmaster child, unix sockets, and signal forwarding are all
/// unix machinery, so the dev subcommand only exists there.
pub const DEV_USAGE: &str = "usage: zou dev <target> [--pg-bin <dir>] [--port <n>] [--http <n>] [--ops <n>] [--runtime <dir>]";

fn usage() -> ExitCode {
    eprintln!("zou {}", env!("CARGO_PKG_VERSION"));
    eprintln!("{DEV_USAGE}");
    eprintln!("       {}", branch::USAGE);
    eprintln!("       {}", codegen::USAGE);
    eprintln!("       {}", compact::USAGE);
    eprintln!("       {}", info::USAGE);
    eprintln!("       {}", inspect::USAGE);
    #[cfg(unix)]
    eprintln!("       {}", inbox::USAGE);
    eprintln!("       {}", map::USAGE);
    #[cfg(unix)]
    eprintln!("       {}", serve::USAGE);
    eprintln!("       {}", shard::USAGE);
    eprintln!("       {}", stats::USAGE);
    eprintln!("       {}", sync::PUSH_USAGE);
    eprintln!("       {}", sync::PULL_USAGE);
    eprintln!("       {}", tenant::USAGE);
    eprintln!("       zou --version");
    ExitCode::from(2)
}

fn simple(result: Result<(), String>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("zou: {e}");
            ExitCode::FAILURE
        }
    }
}

fn main() -> ExitCode {
    // Logs on stderr, RUST_LOG filters them, info by default, and
    // ZOU_LOG_FORMAT=json spells them as json lines for a collector.
    // Results meant for scripts stay on stdout untouched.
    zou_ops::logs::init("info");
    // And traces, but only when ZOU_OTLP_ENDPOINT names a collector.
    // Nothing set and there is no thread and no span, which is what a
    // one shot command like `zou stats` should pay for tracing.
    zou_ops::trace::from_env("zou");
    let argv: Vec<String> = std::env::args().skip(1).collect();
    match argv.first().map(String::as_str) {
        Some("--version") | Some("-V") => {
            println!("zou {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        #[cfg(unix)]
        Some("dev") => {
            let args = match dev::parse(&argv[1..]) {
                Ok(args) => args,
                Err(e) => {
                    eprintln!("zou: {e}");
                    return ExitCode::from(2);
                }
            };
            match dev::run(&args) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("zou: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        #[cfg(not(unix))]
        Some("dev") => {
            eprintln!("zou: dev needs a unix platform");
            ExitCode::FAILURE
        }
        Some("branch") => simple(branch::run(&argv[1..])),
        Some("compact") => simple(compact::run(&argv[1..])),
        Some("gen") => simple(codegen::run(&argv[1..])),
        #[cfg(unix)]
        Some("inbox") => simple(inbox::run(&argv[1..])),
        Some("info") => simple(info::run(&argv[1..])),
        Some("inspect") => simple(inspect::run(&argv[1..])),
        Some("map") => simple(map::run(&argv[1..])),
        #[cfg(unix)]
        Some("serve") => {
            let args = match serve::parse(&argv[1..]) {
                Ok(args) => args,
                Err(e) => {
                    eprintln!("zou: {e}");
                    return ExitCode::from(2);
                }
            };
            simple(serve::run(&args))
        }
        #[cfg(not(unix))]
        Some("serve") => {
            eprintln!("zou: serve needs a unix platform");
            ExitCode::FAILURE
        }
        Some("shard") => simple(shard::run(&argv[1..])),
        Some("push") => simple(sync::push(&argv[1..])),
        Some("pull") => simple(sync::pull(&argv[1..])),
        Some("stats") => simple(stats::run(&argv[1..])),
        Some("tenant") => simple(tenant::run(&argv[1..])),
        _ => usage(),
    }
}
