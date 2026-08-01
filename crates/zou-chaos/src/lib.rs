//! A fault injecting HTTP proxy that sits between a store client and an
//! S3 compatible endpoint. Faults fire on deterministic counters, every
//! Nth request, so a test run is reproducible: no clocks, no randomness.
//!
//! Three faults cover the matrix rows the design doc promises:
//! - an injected 503 answer, the throttling and transient server error
//!   case the client must absorb with bounded retries
//! - a latency spike before forwarding, the slow endpoint case
//! - a truncated PUT, forwarding only half the body and dropping both
//!   sockets, the partial upload case where the endpoint must keep the
//!   old object and the caller must see a hard error
//!
//! Requests are forwarded byte for byte except the hop by hop Connection
//! header, so SigV4 signatures stay valid: the endpoint checks the Host
//! header it receives, and that still names the proxy the client signed.
//! Only Content-Length bodies are supported, which is all the zou-store
//! client sends. std only, one thread per connection.

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Fault schedule. A zero disables that fault.
#[derive(Debug, Clone)]
pub struct ChaosConfig {
    /// `host:port` of the real endpoint.
    pub upstream: String,
    /// Answer every Nth request with a 503 without touching upstream.
    pub error_every: u64,
    /// Sleep before forwarding every Nth request.
    pub delay_every: u64,
    pub delay_ms: u64,
    /// Forward only half the body of every Nth PUT, then drop both
    /// sockets. Counts PUTs, not requests.
    pub truncate_put_every: u64,
}

/// A running proxy. Dropping the handle leaves the accept thread
/// serving until the process ends, which is what tests want.
pub struct ChaosProxy {
    addr: SocketAddr,
}

impl ChaosProxy {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

/// Bind `listen` (use port 0 for an ephemeral port) and serve in
/// background threads.
pub fn spawn(listen: &str, cfg: ChaosConfig) -> std::io::Result<ChaosProxy> {
    let listener = TcpListener::bind(listen)?;
    let addr = listener.local_addr()?;
    let requests = Arc::new(AtomicU64::new(0));
    let puts = Arc::new(AtomicU64::new(0));
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(client) = conn else { continue };
            let cfg = cfg.clone();
            let requests = Arc::clone(&requests);
            let puts = Arc::clone(&puts);
            std::thread::spawn(move || {
                let _ = handle(client, &cfg, &requests, &puts);
            });
        }
    });
    Ok(ChaosProxy { addr })
}

fn other(msg: &str) -> std::io::Error {
    std::io::Error::other(msg.to_string())
}

/// Read until the blank line ending the header block. Returns the head
/// including its trailing CRLFCRLF, plus any body bytes already read.
fn read_head(stream: &mut TcpStream) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        if let Some(pos) = find(&buf, b"\r\n\r\n") {
            let body = buf.split_off(pos + 4);
            return Ok((buf, body));
        }
        if buf.len() > 1 << 20 {
            return Err(other("request head over 1 MiB"));
        }
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Err(other("connection closed inside the head"));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn header_value(head: &[u8], name: &str) -> Option<String> {
    let text = String::from_utf8_lossy(head);
    for line in text.split("\r\n").skip(1) {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        if k.trim().eq_ignore_ascii_case(name) {
            return Some(v.trim().to_string());
        }
    }
    None
}

/// The forwarded head keeps every byte the client signed and swaps only
/// the hop by hop Connection header for `close`, so the upstream answer
/// ends at EOF and relaying needs no response parsing.
fn rewrite_head(head: &[u8]) -> Vec<u8> {
    let text = String::from_utf8_lossy(head);
    let mut out = String::new();
    for line in text.split("\r\n") {
        if line.is_empty() || line.to_ascii_lowercase().starts_with("connection:") {
            continue;
        }
        out.push_str(line);
        out.push_str("\r\n");
    }
    out.push_str("connection: close\r\n\r\n");
    out.into_bytes()
}

fn respond_503(client: &mut TcpStream) -> std::io::Result<()> {
    let body = b"<Error><Code>SlowDown</Code><Message>injected by zou-chaos</Message></Error>";
    write!(
        client,
        "HTTP/1.1 503 Service Unavailable\r\ncontent-type: application/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    )?;
    client.write_all(body)
}

fn handle(
    mut client: TcpStream,
    cfg: &ChaosConfig,
    requests: &AtomicU64,
    puts: &AtomicU64,
) -> std::io::Result<()> {
    let (head, mut body) = read_head(&mut client)?;
    let method = String::from_utf8_lossy(&head)
        .split(' ')
        .next()
        .unwrap_or("")
        .to_string();
    let content_length: usize = header_value(&head, "content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    while body.len() < content_length {
        let mut chunk = [0u8; 16 * 1024];
        let n = client.read(&mut chunk)?;
        if n == 0 {
            return Err(other("connection closed inside the body"));
        }
        body.extend_from_slice(&chunk[..n]);
    }

    let n = requests.fetch_add(1, Ordering::SeqCst) + 1;
    if cfg.error_every > 0 && n.is_multiple_of(cfg.error_every) {
        return respond_503(&mut client);
    }
    if cfg.delay_every > 0 && n.is_multiple_of(cfg.delay_every) {
        std::thread::sleep(Duration::from_millis(cfg.delay_ms));
    }
    if method == "PUT" {
        let p = puts.fetch_add(1, Ordering::SeqCst) + 1;
        if cfg.truncate_put_every > 0 && p.is_multiple_of(cfg.truncate_put_every) {
            let mut upstream = TcpStream::connect(&cfg.upstream)?;
            upstream.write_all(&rewrite_head(&head))?;
            upstream.write_all(&body[..body.len() / 2])?;
            // Content-Length promised more, so the endpoint must abort
            // the write. The client gets no answer at all.
            let _ = upstream.shutdown(Shutdown::Both);
            let _ = client.shutdown(Shutdown::Both);
            return Ok(());
        }
    }

    let mut upstream = TcpStream::connect(&cfg.upstream)?;
    upstream.write_all(&rewrite_head(&head))?;
    upstream.write_all(&body)?;
    let mut answer = Vec::new();
    upstream.read_to_end(&mut answer)?;
    client.write_all(&answer)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal upstream answering every request with 200 and echoing
    /// how many bytes of body it received.
    fn stub_upstream() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(mut stream) = conn else { continue };
                std::thread::spawn(move || {
                    let Ok((head, mut body)) = read_head(&mut stream) else {
                        return;
                    };
                    let want: usize = header_value(&head, "content-length")
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);
                    let mut chunk = [0u8; 4096];
                    while body.len() < want {
                        match stream.read(&mut chunk) {
                            Ok(0) | Err(_) => return,
                            Ok(n) => body.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let reply = format!("got {} bytes", body.len());
                    let _ = write!(
                        stream,
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
                        reply.len()
                    );
                });
            }
        });
        addr
    }

    fn roundtrip(addr: SocketAddr, request: &str) -> String {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(request.as_bytes()).unwrap();
        let mut answer = Vec::new();
        stream.read_to_end(&mut answer).unwrap();
        String::from_utf8_lossy(&answer).to_string()
    }

    #[test]
    fn every_second_request_gets_a_503_and_the_rest_pass_through() {
        let upstream = stub_upstream();
        let proxy = spawn(
            "127.0.0.1:0",
            ChaosConfig {
                upstream: upstream.to_string(),
                error_every: 2,
                delay_every: 0,
                delay_ms: 0,
                truncate_put_every: 0,
            },
        )
        .unwrap();
        let get = "GET /k HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\n\r\n";
        assert!(roundtrip(proxy.addr(), get).starts_with("HTTP/1.1 200"));
        assert!(roundtrip(proxy.addr(), get).starts_with("HTTP/1.1 503"));
        assert!(roundtrip(proxy.addr(), get).starts_with("HTTP/1.1 200"));
    }

    #[test]
    fn a_truncated_put_reaches_upstream_short_and_answers_nobody() {
        let upstream = stub_upstream();
        let proxy = spawn(
            "127.0.0.1:0",
            ChaosConfig {
                upstream: upstream.to_string(),
                error_every: 0,
                delay_every: 0,
                delay_ms: 0,
                truncate_put_every: 2,
            },
        )
        .unwrap();
        let put = format!(
            "PUT /k HTTP/1.1\r\nhost: x\r\ncontent-length: 8\r\n\r\n{}",
            "01234567"
        );
        // First PUT passes whole.
        assert!(roundtrip(proxy.addr(), &put).contains("got 8 bytes"));
        // Second PUT is cut: the client reads EOF without any response.
        assert_eq!(roundtrip(proxy.addr(), &put), "");
        // The counter is per PUT, so the third passes again.
        assert!(roundtrip(proxy.addr(), &put).contains("got 8 bytes"));
    }
}
