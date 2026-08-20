//! `zou inbox`: read the mail the dev loop kept, and clear it.
//!
//! A project with no mail server configured keeps what it sends in
//! memory instead of dropping it, and serves it from `/dev/inbox` to
//! the service role. This is that endpoint with a terminal in front of
//! it: run it after a signup and the confirmation link is on screen,
//! ready to paste into a browser. No container, no second port, no
//! mail catcher.
//!
//! It talks to 127.0.0.1 and nothing else, on purpose. The mailbox is
//! part of the local loop, and a command that could point at a remote
//! project would be a command for reading other people's codes.

use std::io::{Read, Write};
use std::net::TcpStream;

pub const USAGE: &str = "usage: zou inbox [--http <n>] [--clear] [--json]";

/// The port supabase start puts its api on, which is the one a client
/// in a dev loop is already pointed at.
const DEFAULT_PORT: u16 = 54321;

pub struct Args {
    pub port: u16,
    pub clear: bool,
    pub json: bool,
}

pub fn parse(argv: &[String]) -> Result<Args, String> {
    let mut args = Args {
        port: DEFAULT_PORT,
        clear: false,
        json: false,
    };
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--http" => {
                let raw = it.next().ok_or_else(|| "--http needs a port".to_string())?;
                args.port = raw.parse().map_err(|_| {
                    format!("bad http port {raw:?}, write a port number from 1 to 65535")
                })?;
            }
            "--clear" => args.clear = true,
            "--json" => args.json = true,
            other => return Err(format!("unexpected {other:?}\n{USAGE}")),
        }
    }
    Ok(args)
}

/// The key the mailbox answers to. ZOU_SERVICE_KEY when the caller has
/// one to hand, otherwise minted from the secret `zou dev` asks to be
/// pinned, which is the same key it printed at startup.
fn service_key() -> Result<String, String> {
    if let Ok(key) = std::env::var("ZOU_SERVICE_KEY")
        && !key.is_empty()
    {
        return Ok(key);
    }
    match std::env::var("ZOU_JWT_SECRET") {
        Ok(secret) if !secret.is_empty() => Ok(zou_server::jwt::mint(
            &zou_server::jwt::key_claims("service_role"),
            secret.as_bytes(),
        )),
        _ => Err("no service key: set ZOU_SERVICE_KEY, or pin ZOU_JWT_SECRET the way zou dev asks and this will mint one".to_string()),
    }
}

pub fn run(argv: &[String]) -> Result<(), String> {
    let args = parse(argv)?;
    let key = service_key()?;
    let method = match args.clear {
        true => "DELETE",
        false => "GET",
    };
    let body = ask(args.port, method, &key)?;
    if args.json {
        println!("{body}");
        return Ok(());
    }
    let parsed: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| format!("the inbox answered with {e}: {body}"))?;
    let messages = parsed["messages"]
        .as_array()
        .ok_or_else(|| format!("the inbox answered with {body}"))?;
    let empty = Vec::new();
    let texts = parsed["texts"].as_array().unwrap_or(&empty);
    if args.clear {
        println!("inbox cleared");
        return Ok(());
    }
    if messages.is_empty() && texts.is_empty() {
        println!("no mail");
        return Ok(());
    }
    let now = now();
    for message in messages {
        print!("{}", render(message, now));
    }
    for text in texts {
        print!("{}", texted(text, now));
    }
    Ok(())
}

/// One text the way a person reads it. There is no link and no subject,
/// only the number and the code, so the code is the line.
fn texted(text: &serde_json::Value, now: i64) -> String {
    format!(
        "{} to {}\n  {}\n",
        age(now - text["at"].as_i64().unwrap_or(now)),
        text["to"].as_str().unwrap_or(""),
        text["body"].as_str().unwrap_or(""),
    )
}

/// One message the way a person reads it: who it went to and when, the
/// subject, and then the link on a line of its own because that is
/// what the whole exercise is for.
fn render(message: &serde_json::Value, now: i64) -> String {
    let text = |key: &str| message[key].as_str().unwrap_or("").to_string();
    let mut out = format!(
        "{} to {}\n  {}\n",
        age(now - message["at"].as_i64().unwrap_or(now)),
        text("to"),
        text("subject"),
    );
    if let Some(link) = message["link"].as_str() {
        out.push_str(&format!("  {link}\n"));
    }
    out
}

/// How long ago, in the largest unit that still says something.
fn age(seconds: i64) -> String {
    match seconds {
        s if s < 0 => "just now".to_string(),
        s if s < 60 => format!("{s}s ago"),
        s if s < 3600 => format!("{}m ago", s / 60),
        s => format!("{}h ago", s / 3600),
    }
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// One request to the dev inbox, answered in full.
///
/// This is a hand written client because it is the only http this
/// binary speaks and it speaks it to the loopback address: pulling in
/// a client library and a tls stack to reach 127.0.0.1 would be a
/// larger dependency than the command.
fn ask(port: u16, method: &str, key: &str) -> Result<String, String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .map_err(|e| format!("no server on 127.0.0.1:{port}: {e}"))?;
    let request = format!(
        "{method} /dev/inbox HTTP/1.1\r\n\
         Host: 127.0.0.1:{port}\r\n\
         apikey: {key}\r\n\
         Accept: application/json\r\n\
         Connection: close\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("write: {e}"))?;
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .map_err(|e| format!("read: {e}"))?;
    // Split on bytes rather than on text: a chunk length counts bytes,
    // and a subject line is allowed to be in any language.
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| "the server answered with nothing".to_string())?;
    let head = String::from_utf8_lossy(&raw[..split]).to_string();
    let body = &raw[split + 4..];
    let body = match head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        true => String::from_utf8_lossy(&dechunk(body)).to_string(),
        false => String::from_utf8_lossy(body).to_string(),
    };
    match status(&head) {
        Some(200) => Ok(body),
        Some(404) => Err(format!(
            "no inbox on 127.0.0.1:{port}: either the key is not the service role key, or this project has a real mail server and a real mailbox to read"
        )),
        Some(code) => Err(format!("the server answered {code}: {body}")),
        None => Err("the server answered with no status".to_string()),
    }
}

fn status(head: &str) -> Option<u16> {
    head.lines().next()?.split_whitespace().nth(1)?.parse().ok()
}

/// Chunked bodies, in case a proxy or a future hyper decides to send
/// one. Each chunk is a hex length, the chunk, and a blank line.
fn dechunk(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(eol) = rest.windows(2).position(|w| w == b"\r\n") {
        let header = String::from_utf8_lossy(&rest[..eol]).to_string();
        let size = match usize::from_str_radix(header.split(';').next().unwrap_or("").trim(), 16) {
            Ok(0) | Err(_) => break,
            Ok(size) => size,
        };
        let chunk = &rest[eol + 2..];
        if chunk.len() < size {
            break;
        }
        out.extend_from_slice(&chunk[..size]);
        rest = &chunk[size..];
        while rest.starts_with(b"\r\n") {
            rest = &rest[2..];
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn the_port_has_a_default_and_the_flags_are_off() {
        let args = parse(&argv(&[])).expect("no arguments is the usual case");
        assert_eq!(args.port, DEFAULT_PORT);
        assert!(!args.clear);
        assert!(!args.json);
        let args = parse(&argv(&["--http", "9999", "--clear", "--json"])).expect("all of them");
        assert_eq!(args.port, 9999);
        assert!(args.clear && args.json);
        assert!(parse(&argv(&["--http"])).is_err());
        assert!(parse(&argv(&["--http", "cold"])).is_err());
        assert!(parse(&argv(&["--wat"])).is_err());
    }

    #[test]
    fn a_message_prints_with_its_link_on_its_own_line() {
        let message = serde_json::json!({
            "to": "someone@zou.test",
            "subject": "Confirm your email address",
            "link": "http://127.0.0.1:54321/auth/v1/verify?token=abc&type=signup",
            "at": 1_000,
        });
        assert_eq!(
            render(&message, 1_090),
            "1m ago to someone@zou.test\n  \
             Confirm your email address\n  \
             http://127.0.0.1:54321/auth/v1/verify?token=abc&type=signup\n"
        );
    }

    #[test]
    fn a_message_with_nothing_to_click_prints_without_a_link() {
        let message = serde_json::json!({
            "to": "someone@zou.test",
            "subject": "123456 is your verification code",
            "link": serde_json::Value::Null,
            "at": 1_000,
        });
        assert_eq!(
            render(&message, 1_005),
            "5s ago to someone@zou.test\n  123456 is your verification code\n"
        );
    }

    #[test]
    fn a_text_prints_its_number_and_the_code_that_went_to_it() {
        let text = serde_json::json!({
            "to": "15551234567",
            "body": "Your code is 123456",
            "code": "123456",
            "channel": "sms",
            "at": 1_000,
        });
        assert_eq!(
            texted(&text, 1_030),
            "30s ago to 15551234567\n  Your code is 123456\n"
        );
    }

    #[test]
    fn the_status_line_is_read_and_a_chunked_body_is_put_back_together() {
        assert_eq!(status("HTTP/1.1 200 OK\r\ncontent-length: 2"), Some(200));
        assert_eq!(status("garbage"), None);
        assert_eq!(
            dechunk(b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"),
            b"hello world"
        );
        assert_eq!(dechunk(b"2\r\n{}\r\n0\r\n\r\n"), b"{}");
    }
}
