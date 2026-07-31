mod branch;
#[cfg(unix)]
mod dev;
mod info;

use std::process::ExitCode;

/// The postmaster child, unix sockets, and signal forwarding are all
/// unix machinery, so the dev subcommand only exists there.
pub const DEV_USAGE: &str =
    "usage: zou dev <target> [--pg-bin <dir>] [--port <n>] [--runtime <dir>]";

fn usage() -> ExitCode {
    eprintln!("zou {}", env!("CARGO_PKG_VERSION"));
    eprintln!("{DEV_USAGE}");
    eprintln!("       {}", branch::USAGE);
    eprintln!("       {}", info::USAGE);
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
    // Structured logs on stderr, RUST_LOG filters them, info by default.
    // Results meant for scripts stay on stdout untouched.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
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
        Some("info") => simple(info::run(&argv[1..])),
        _ => usage(),
    }
}
