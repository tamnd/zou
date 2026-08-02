//! `zou gen types typescript`: the database as a TypeScript file.
//!
//! supabase-js is generic over a `Database` type, and the whole of its
//! type safety comes from a file generated out of the catalog. That
//! file has a shape clients depend on, down to where the line breaks
//! are, since it is checked into their repositories and shows up in
//! their diffs. So this writes the same file, byte for byte, rather
//! than an equivalent one.
//!
//! Three pieces: `catalog` reads, `typescript` decides what the types
//! are, and `pretty` decides where the lines end.

mod catalog;
mod pretty;
mod sort;
mod typescript;

pub const USAGE: &str =
    "usage: zou gen types typescript [--db-url <url>] [--schema <name>]... [--output <path>]";

#[derive(Debug)]
pub struct Args {
    pub url: String,
    pub schemas: Vec<String>,
    pub output: Option<String>,
}

pub fn parse(argv: &[String]) -> Result<Args, String> {
    let mut it = argv.iter();
    match it.next().map(String::as_str) {
        Some("types") => {}
        Some(other) => return Err(format!("unexpected {other:?}\n{USAGE}")),
        None => return Err(format!("gen needs something to generate\n{USAGE}")),
    }
    match it.next().map(String::as_str) {
        Some("typescript") => {}
        Some(other) => return Err(format!("cannot generate types for {other:?}\n{USAGE}")),
        None => return Err(format!("gen types needs a language\n{USAGE}")),
    }

    let mut url = None;
    let mut schemas: Vec<String> = Vec::new();
    let mut output = None;
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--db-url" => {
                let raw = it
                    .next()
                    .ok_or_else(|| "--db-url needs a url".to_string())?;
                url = Some(raw.clone());
            }
            // Repeatable and comma separated both, since supabase's cli
            // takes it either way and a Makefile has usually picked one.
            "--schema" => {
                let raw = it
                    .next()
                    .ok_or_else(|| "--schema needs a name".to_string())?;
                schemas.extend(raw.split(',').filter(|s| !s.is_empty()).map(str::to_string));
            }
            "--output" | "-o" => {
                let raw = it
                    .next()
                    .ok_or_else(|| "--output needs a path".to_string())?;
                output = Some(raw.clone());
            }
            other => return Err(format!("unexpected {other:?}\n{USAGE}")),
        }
    }
    let url = url
        .or_else(|| non_empty("ZOU_DB_URL"))
        .or_else(|| non_empty("DATABASE_URL"))
        .ok_or_else(|| format!("no database to read: pass --db-url, or set ZOU_DB_URL\n{USAGE}"))?;
    if schemas.is_empty() {
        schemas.push("public".to_string());
    }
    Ok(Args {
        url,
        schemas,
        output,
    })
}

fn non_empty(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Some(value),
        _ => None,
    }
}

pub fn run(argv: &[String]) -> Result<(), String> {
    let args = parse(argv)?;
    let file = generate(&args.url, &args.schemas)?;
    match &args.output {
        Some(path) => std::fs::write(path, &file).map_err(|e| format!("cannot write {path}: {e}")),
        None => {
            print!("{file}");
            Ok(())
        }
    }
}

/// Reading the catalog is the only async part, and the command is over
/// once it is done, so the runtime lives no longer than the read.
pub fn generate(url: &str, schemas: &[String]) -> Result<String, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("cannot start a runtime: {e}"))?;
    let catalog = runtime.block_on(catalog::read(url, schemas))?;
    Ok(typescript::render(&catalog))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|a| a.to_string()).collect()
    }

    #[test]
    fn a_url_on_the_command_line_is_the_one_used() {
        let args = parse(&argv(&["types", "typescript", "--db-url", "postgres:///x"])).unwrap();
        assert_eq!(args.url, "postgres:///x");
        assert_eq!(args.schemas, ["public"]);
        assert!(args.output.is_none());
    }

    #[test]
    fn schemas_can_be_given_one_at_a_time_or_all_at_once() {
        let args = parse(&argv(&[
            "types",
            "typescript",
            "--db-url",
            "postgres:///x",
            "--schema",
            "public,shop",
            "--schema",
            "auth",
        ]))
        .unwrap();
        assert_eq!(args.schemas, ["public", "shop", "auth"]);
    }

    #[test]
    fn a_language_that_is_not_typescript_is_refused() {
        let e = parse(&argv(&["types", "go", "--db-url", "postgres:///x"])).unwrap_err();
        assert!(e.contains("\"go\""), "{e}");
    }

    #[test]
    fn a_missing_language_says_so_rather_than_guessing() {
        let e = parse(&argv(&["types"])).unwrap_err();
        assert!(e.contains("needs a language"), "{e}");
    }

    #[test]
    fn a_flag_with_nothing_after_it_is_an_error() {
        let e = parse(&argv(&["types", "typescript", "--schema"])).unwrap_err();
        assert_eq!(e, "--schema needs a name");
    }
}
