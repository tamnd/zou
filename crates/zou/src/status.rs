//! `zou status`: what a client should be pointed at, in the shape
//! `supabase status` prints it.
//!
//! A project's tooling reads these lines, so they are the same lines,
//! and `-o env` is the form a shell evals. The ports come from
//! `supabase/config.toml` when the project has one, the keys are minted
//! from ZOU_JWT_SECRET, and the api port is probed, so the command
//! doubles as a readiness check: nothing listening is a failure exit
//! and a line on stderr, which is what a script waiting for the server
//! wants.
//!
//! The keys can only be printed when the secret is pinned. `zou dev`
//! with nothing set generates one and logs it, and a generated secret
//! is not knowable from here, which the output says rather than
//! printing a key that signs nothing.

use std::net::{Shutdown, SocketAddr, TcpStream};
use std::path::PathBuf;
use std::time::Duration;

use crate::config::{self, Project};

pub const USAGE: &str =
    "usage: zou status [--config <config.toml>] [--api <port>] [--db <port>] [-o pretty|env|json]";

/// How long a probe waits before calling the port shut. Loopback
/// answers in microseconds or not at all.
const PROBE: Duration = Duration::from_millis(300);

#[derive(PartialEq)]
enum Output {
    Pretty,
    Env,
    Json,
}

struct Args {
    config: Option<PathBuf>,
    api: Option<u16>,
    db: Option<u16>,
    output: Output,
}

fn parse(argv: &[String]) -> Result<Args, String> {
    let mut args = Args {
        config: None,
        api: None,
        db: None,
        output: Output::Pretty,
    };
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        let mut need = |flag: &str| {
            it.next()
                .cloned()
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        match arg.as_str() {
            "--config" => args.config = Some(PathBuf::from(need("--config")?)),
            "--api" => {
                let raw = need("--api")?;
                args.api = Some(raw.parse().map_err(|_| {
                    format!("bad port {raw:?}, write a port number from 1 to 65535")
                })?);
            }
            "--db" => {
                let raw = need("--db")?;
                args.db = Some(raw.parse().map_err(|_| {
                    format!("bad port {raw:?}, write a port number from 1 to 65535")
                })?);
            }
            "-o" | "--output" => {
                let raw = need("-o")?;
                args.output = match raw.as_str() {
                    "pretty" => Output::Pretty,
                    "env" => Output::Env,
                    "json" => Output::Json,
                    other => return Err(format!("unknown output {other:?}\n{USAGE}")),
                };
            }
            other => return Err(format!("unexpected argument {other:?}\n{USAGE}")),
        }
    }
    Ok(args)
}

/// The ports `zou dev` uses with no flags and no project file.
const DEFAULT_API: u16 = 54321;
const DEFAULT_DB: u16 = 5432;

fn listening(port: u16) -> bool {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    match TcpStream::connect_timeout(&addr, PROBE) {
        Ok(sock) => {
            let _ = sock.shutdown(Shutdown::Both);
            true
        }
        Err(_) => false,
    }
}

struct Status {
    api: u16,
    db: u16,
    secret: Option<String>,
    /// The pair the S3 endpoint is asked with, which is knowable from
    /// here even when the JWT secret is not: it is fixed unless the
    /// environment named another, and both cases are the same rule
    /// `zou dev` applied when it started. None is a project whose own
    /// file switched the endpoint off, and printing a pair for that
    /// would be printing a key to a door that is not there.
    s3: Option<zou_server::s3::Credentials>,
    project: Option<Project>,
}

/// The names this project would serve, or nothing at all when the
/// directory cannot be read: `zou status` prints what is there and does
/// not become the thing that reports a broken disk.
fn functions(project: &Project) -> Vec<String> {
    zou_functions::read(&project.dir(), &project.functions)
        .unwrap_or_default()
        .into_iter()
        .map(|f| f.name)
        .collect()
}

impl Status {
    fn api_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.api)
    }

    fn s3_url(&self) -> String {
        format!("http://127.0.0.1:{}/storage/v1/s3", self.api)
    }

    fn db_url(&self) -> String {
        config::local_db_url(self.db)
    }

    fn keys(&self) -> Option<(String, String)> {
        let secret = self.secret.as_ref()?;
        Some((
            zou_server::jwt::mint(&zou_server::jwt::key_claims("anon"), secret.as_bytes()),
            zou_server::jwt::mint(
                &zou_server::jwt::key_claims("service_role"),
                secret.as_bytes(),
            ),
        ))
    }

    fn pretty(&self) {
        let (anon, service) = self.keys().unzip();
        let unknown = "unknown, pin ZOU_JWT_SECRET and restart to see it".to_string();
        let mut lines = vec![
            ("API URL", self.api_url()),
            ("S3 Storage URL", self.s3_url()),
            ("DB URL", self.db_url()),
            (
                "JWT secret",
                self.secret.clone().unwrap_or_else(|| unknown.clone()),
            ),
            ("anon key", anon.unwrap_or_else(|| unknown.clone())),
            ("service_role key", service.unwrap_or(unknown)),
        ];
        match &self.s3 {
            Some(s3) => lines.extend([
                ("S3 Access Key", s3.access.clone()),
                ("S3 Secret Key", s3.secret.clone()),
                ("S3 Region", s3.region.clone()),
            ]),
            None => lines.push((
                "S3 endpoint",
                "off, storage.s3_protocol.enabled is false".to_string(),
            )),
        }
        for (label, value) in lines {
            println!("{label:>16}: {value}");
        }
        let Some(project) = &self.project else {
            return;
        };
        println!("{:>16}: {}", "config", project.path.display());
        let served = functions(project);
        if !served.is_empty() {
            println!(
                "{:>16}: {} on {}",
                "functions",
                served.join(", "),
                crate::functions::engine_describe(project.functions.policy)
            );
        }
        if !project.unread.is_empty() {
            println!("{:>16}: {}", "not read yet", project.unread.join(", "));
        }
    }

    fn env(&self) {
        let (anon, service) = self.keys().unzip();
        println!("API_URL=\"{}\"", self.api_url());
        println!("STORAGE_S3_URL=\"{}\"", self.s3_url());
        println!("DB_URL=\"{}\"", self.db_url());
        if let Some(secret) = &self.secret {
            println!("JWT_SECRET=\"{secret}\"");
        }
        if let (Some(anon), Some(service)) = (anon, service) {
            println!("ANON_KEY=\"{anon}\"");
            println!("SERVICE_ROLE_KEY=\"{service}\"");
        }
        // The names are the CLI's, not ours: a project's scripts read
        // these out of `supabase status -o env` today, and a rename
        // here would be a script to edit for no reason.
        if let Some(s3) = &self.s3 {
            println!("S3_PROTOCOL_ACCESS_KEY_ID=\"{}\"", s3.access);
            println!("S3_PROTOCOL_ACCESS_KEY_SECRET=\"{}\"", s3.secret);
            println!("S3_PROTOCOL_REGION=\"{}\"", s3.region);
        }
    }

    fn json(&self) {
        let (anon, service) = self.keys().unzip();
        let mut out = serde_json::json!({
            "api_url": self.api_url(),
            "storage_s3_url": self.s3_url(),
            "db_url": self.db_url(),
            "jwt_secret": self.secret,
            "anon_key": anon,
            "service_role_key": service,
            "s3_access_key": self.s3.as_ref().map(|s3| &s3.access),
            "s3_secret_key": self.s3.as_ref().map(|s3| &s3.secret),
            "s3_region": self.s3.as_ref().map(|s3| &s3.region),
        });
        if let Some(project) = &self.project {
            out["config"] = serde_json::json!(project.path.display().to_string());
            out["project_id"] = serde_json::json!(project.id);
            out["unread"] = serde_json::json!(project.unread);
            out["functions"] = serde_json::json!(functions(project));
            out["functions_engine"] =
                serde_json::json!(crate::functions::engine_describe(project.functions.policy));
        }
        println!("{out}");
    }
}

pub fn run(argv: &[String]) -> Result<(), String> {
    let args = parse(argv)?;
    let project = config::locate(args.config.as_deref())?;
    let from_project = |pick: fn(&Project) -> Option<u16>| project.as_ref().and_then(pick);
    let status = Status {
        api: args
            .api
            .or_else(|| from_project(|p| p.api))
            .unwrap_or(DEFAULT_API),
        db: args
            .db
            .or_else(|| from_project(|p| p.db))
            .unwrap_or(DEFAULT_DB),
        secret: std::env::var("ZOU_JWT_SECRET")
            .ok()
            .filter(|s| !s.is_empty()),
        // The same rule `zou dev` applied when it started: the project's
        // own file decides whether there is an endpoint at all.
        s3: project.as_ref().is_none_or(|p| p.s3).then(config::local_s3),
        project,
    };
    match args.output {
        Output::Pretty => status.pretty(),
        Output::Env => status.env(),
        Output::Json => status.json(),
    }
    // Printed first, so a caller that reads stdout gets the settings
    // whatever the exit code says about the process behind them.
    let mut down = Vec::new();
    if !listening(status.api) {
        down.push(format!("http://127.0.0.1:{}", status.api));
    }
    if !listening(status.db) {
        down.push(format!("127.0.0.1:{}", status.db));
    }
    if down.is_empty() {
        return Ok(());
    }
    Err(format!(
        "nothing is listening on {}, start it with zou dev <target>",
        down.join(" or ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn the_flags_come_apart() {
        let args = parse(&argv(&["--api", "1", "--db", "2", "-o", "json"])).unwrap();
        assert_eq!(args.api, Some(1));
        assert_eq!(args.db, Some(2));
        assert!(args.output == Output::Json);
        assert!(parse(&argv(&["-o", "yaml"])).is_err());
        assert!(parse(&argv(&["--api"])).is_err());
        assert!(parse(&argv(&["--api", "http"])).is_err());
        assert!(parse(&argv(&["status"])).is_err());
    }

    #[test]
    fn the_urls_are_the_ones_a_client_is_given() {
        let status = Status {
            api: 54321,
            db: 5432,
            secret: Some("a secret at least thirty two bytes long".into()),
            s3: Some(config::local_s3()),
            project: None,
        };
        assert_eq!(status.api_url(), "http://127.0.0.1:54321");
        assert_eq!(status.s3_url(), "http://127.0.0.1:54321/storage/v1/s3");
        assert_eq!(
            status.db_url(),
            "postgresql://postgres:postgres@127.0.0.1:5432/postgres"
        );
        let (anon, service) = status.keys().unwrap();
        assert_ne!(anon, service, "two roles, two tokens");
        assert!(anon.starts_with("eyJ"), "and they are jwts, {anon}");
    }

    #[test]
    fn with_no_secret_there_are_no_keys_to_print() {
        let status = Status {
            api: 54321,
            db: 5432,
            secret: None,
            s3: Some(config::local_s3()),
            project: None,
        };
        assert!(status.keys().is_none());
        assert_eq!(
            status.s3.map(|s3| s3.access).as_deref(),
            Some(config::LOCAL_S3_ACCESS_KEY),
            "the S3 pair is fixed, so it is printable when the keys are not"
        );
    }

    #[test]
    fn a_shut_port_is_shut() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        assert!(listening(port));
        drop(listener);
        assert!(!listening(port), "and it is not once nobody is holding it");
    }
}
