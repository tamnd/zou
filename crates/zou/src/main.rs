fn main() {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("--version") | Some("-V") => {
            println!("zou {}", env!("CARGO_PKG_VERSION"));
        }
        _ => {
            eprintln!("zou {}", env!("CARGO_PKG_VERSION"));
            eprintln!("nothing to run yet, see https://github.com/tamnd/zou");
            std::process::exit(2);
        }
    }
}
