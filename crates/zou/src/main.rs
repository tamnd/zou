mod dev;

use std::process::ExitCode;

fn usage() -> ExitCode {
    eprintln!("zou {}", env!("CARGO_PKG_VERSION"));
    eprintln!("{}", dev::USAGE);
    eprintln!("       zou --version");
    ExitCode::from(2)
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    match argv.first().map(String::as_str) {
        Some("--version") | Some("-V") => {
            println!("zou {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
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
        _ => usage(),
    }
}
