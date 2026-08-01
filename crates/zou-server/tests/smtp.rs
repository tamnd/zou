//! The SMTP transport against a mail server that answers.
//!
//! The server is in this file: a socket, a script of replies, and a
//! transcript of everything the client said. That is enough to pin the
//! conversation, and with the fixture certificate it is enough to pin
//! the STARTTLS upgrade too, which is the path every real deployment
//! takes and the one worth being sure about.
//!
//! No postgres and no network, so this runs everywhere the rest of the
//! unit tests do.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{RootCertStore, ServerConnection, StreamOwned};
use zou_server::mail::{CONFIRMATION, Mail, Sender};
use zou_server::smtp::{Security, Smtp};

/// A certificate for localhost, made once with openssl and kept here
/// so the test needs no certificate machinery of its own. It is a self
/// signed CA, trusted by nothing but the client in this file.
const CERT: &[u8] = include_bytes!("fixtures/smtp-localhost.cert.der");
const KEY: &[u8] = include_bytes!("fixtures/smtp-localhost.key.der");

/// What the fake server should do differently from the happy path.
#[derive(Clone, Default)]
struct Opts {
    /// Whether the greeting offers STARTTLS at all.
    no_starttls: bool,
    /// A command to refuse, and the reply to refuse it with.
    refuse: Option<(&'static str, &'static str)>,
    /// TLS from the first byte, which is what port 465 does.
    implicit: bool,
}

/// Either half of the connection, so the same loop can carry on
/// speaking after the upgrade.
enum Io {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ServerConnection, TcpStream>>),
}

impl Read for Io {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Io::Plain(s) => s.read(buf),
            Io::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Io {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Io::Plain(s) => s.write(buf),
            Io::Tls(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Io::Plain(s) => s.flush(),
            Io::Tls(s) => s.flush(),
        }
    }
}

impl Io {
    fn into_plain(self) -> TcpStream {
        match self {
            Io::Plain(s) => s,
            Io::Tls(_) => panic!("already encrypted"),
        }
    }

    /// One line, without its ending. None when the client hung up.
    fn line(&mut self) -> Option<String> {
        let mut out = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            match self.read(&mut byte) {
                Ok(0) | Err(_) => return None,
                Ok(_) => {}
            }
            if byte[0] == b'\n' {
                while out.last() == Some(&b'\r') {
                    out.pop();
                }
                return Some(String::from_utf8_lossy(&out).to_string());
            }
            out.push(byte[0]);
        }
    }

    fn say(&mut self, line: &str) {
        let _ = self.write_all(format!("{line}\r\n").as_bytes());
        let _ = self.flush();
    }
}

fn tls_server(stream: TcpStream) -> Io {
    let config = rustls::ServerConfig::builder_with_provider(
        rustls::crypto::ring::default_provider().into(),
    )
    .with_safe_default_protocol_versions()
    .expect("versions")
    .with_no_client_auth()
    .with_single_cert(
        vec![CertificateDer::from(CERT.to_vec())],
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(KEY.to_vec())),
    )
    .expect("the fixture certificate and key belong together");
    let connection = ServerConnection::new(Arc::new(config)).expect("a server connection");
    Io::Tls(Box::new(StreamOwned::new(connection, stream)))
}

/// A mail server that speaks just enough to take one message, and
/// writes down everything it was told.
fn serve(opts: Opts) -> (u16, JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
    let port = listener.local_addr().expect("bound").port();
    let handle = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("one connection");
        let mut log = Vec::new();
        let mut io = match opts.implicit {
            true => tls_server(stream),
            false => Io::Plain(stream),
        };
        io.say("220 fake.zou.test ESMTP");
        let mut encrypted = opts.implicit;
        while let Some(line) = io.line() {
            log.push(line.clone());
            let word = line.split_whitespace().next().unwrap_or("").to_uppercase();
            if let Some((command, reply)) = opts.refuse
                && word == command
            {
                io.say(reply);
                continue;
            }
            match word.as_str() {
                "EHLO" => {
                    io.say("250-fake.zou.test");
                    if !opts.no_starttls && !encrypted {
                        io.say("250-STARTTLS");
                    }
                    io.say("250 AUTH PLAIN LOGIN");
                }
                "STARTTLS" => {
                    io.say("220 go ahead");
                    io = tls_server(io.into_plain());
                    encrypted = true;
                }
                "AUTH" => io.say("235 2.7.0 accepted"),
                "MAIL" | "RCPT" => io.say("250 2.1.0 ok"),
                "DATA" => {
                    io.say("354 go on");
                    let mut message = String::new();
                    while let Some(line) = io.line() {
                        if line == "." {
                            break;
                        }
                        message.push_str(&line);
                        message.push('\n');
                    }
                    log.push(format!("MESSAGE\n{message}"));
                    io.say("250 2.0.0 queued");
                }
                "QUIT" => {
                    io.say("221 2.0.0 bye");
                    break;
                }
                _ => io.say("500 5.5.1 what"),
            }
        }
        log
    });
    (port, handle)
}

/// A client pointed at the fake server, trusting the fixture and
/// nothing else.
fn client(port: u16) -> Smtp {
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(CERT.to_vec()))
        .expect("the fixture is a certificate");
    Smtp {
        user: "postmaster".to_string(),
        pass: "hunter2".to_string(),
        admin_email: "noreply@zou.test".to_string(),
        sender_name: "Zou".to_string(),
        roots: Arc::new(roots),
        ..Smtp::new("localhost", port)
    }
}

fn mail() -> Mail {
    Mail {
        to: "someone@zou.test".to_string(),
        subject: "Confirm your email address".to_string(),
        body: "<p>Follow the link.</p>".to_string(),
        template: CONFIRMATION.to_string(),
        at: 1_700_000_000,
    }
}

/// The commands, without the message.
fn commands(log: &[String]) -> Vec<String> {
    log.iter()
        .filter(|l| !l.starts_with("MESSAGE"))
        .map(|l| l.split_whitespace().next().unwrap_or("").to_uppercase())
        .collect()
}

fn message(log: &[String]) -> String {
    log.iter()
        .find_map(|l| l.strip_prefix("MESSAGE\n"))
        .expect("a message was sent")
        .to_string()
}

#[test]
fn a_message_goes_out_over_starttls() {
    let (port, server) = serve(Opts::default());
    let smtp = client(port);
    smtp.deliver(&mail()).expect("the message is taken");
    let log = server.join().expect("the server finished");

    assert_eq!(
        commands(&log),
        vec![
            "EHLO", "STARTTLS", "EHLO", "AUTH", "MAIL", "RCPT", "DATA", "QUIT"
        ],
        "the second EHLO is the point: the capability list before the \
         upgrade is not the one to trust"
    );
    let raw = log.iter().find(|l| l.starts_with("AUTH")).expect("an auth");
    let payload = raw.rsplit(' ').next().expect("the credentials");
    use base64ct::Encoding;
    let decoded = base64ct::Base64::decode_vec(payload).expect("base64");
    assert_eq!(
        decoded, b"\0postmaster\0hunter2",
        "AUTH PLAIN is the identity, the user, and the password, nul separated"
    );

    let sent = message(&log);
    assert!(sent.contains("From: Zou <noreply@zou.test>"), "{sent}");
    assert!(sent.contains("To: <someone@zou.test>"), "{sent}");
    assert!(
        sent.contains("Subject: Confirm your email address"),
        "{sent}"
    );
    assert!(
        sent.contains("Date: Tue, 14 Nov 2023 22:13:20 +0000"),
        "{sent}"
    );
    let body: String = sent.split("\n\n").nth(1).expect("a body").lines().collect();
    let decoded = base64ct::Base64::decode_vec(&body).expect("base64");
    assert_eq!(
        String::from_utf8(decoded).unwrap(),
        "<p>Follow the link.</p>"
    );
}

#[test]
fn tls_from_the_first_byte_is_the_other_way_in() {
    let (port, server) = serve(Opts {
        implicit: true,
        ..Opts::default()
    });
    let mut smtp = client(port);
    smtp.security = Security::Implicit;
    smtp.deliver(&mail()).expect("the message is taken");
    let log = server.join().expect("the server finished");
    assert_eq!(
        commands(&log),
        vec!["EHLO", "AUTH", "MAIL", "RCPT", "DATA", "QUIT"],
        "nothing to upgrade, so nothing is said about it"
    );
    assert!(message(&log).contains("Subject: Confirm your email address"));
}

#[test]
fn a_server_that_offers_no_encryption_is_refused_rather_than_indulged() {
    let (port, server) = serve(Opts {
        no_starttls: true,
        ..Opts::default()
    });
    let smtp = client(port);
    let refusal = smtp.deliver(&mail()).expect_err("nothing is sent");
    assert!(
        refusal.contains("offered no starttls"),
        "the password would have gone next: {refusal}"
    );
    let log = server.join().expect("the server finished");
    assert_eq!(
        commands(&log),
        vec!["EHLO"],
        "and it stopped before saying anything else"
    );
}

#[test]
fn a_plain_relay_on_the_loopback_is_allowed_to_take_the_password() {
    // A mail catcher in a container next door, which is how most of
    // the world runs one locally. Nothing goes on a wire, so nothing
    // is refused.
    let (port, server) = serve(Opts {
        no_starttls: true,
        ..Opts::default()
    });
    let mut smtp = client(port);
    smtp.security = Security::None;
    smtp.deliver(&mail()).expect("the message is taken");
    let log = server.join().expect("the server finished");
    assert_eq!(
        commands(&log),
        vec!["EHLO", "AUTH", "MAIL", "RCPT", "DATA", "QUIT"]
    );
}

#[test]
fn a_refusal_is_reported_with_what_the_server_said() {
    for (command, reply, expected) in [
        ("AUTH", "535 5.7.8 bad credentials", "535"),
        ("RCPT", "550 5.1.1 no such mailbox", "550"),
        ("DATA", "552 5.3.4 message too big", "552"),
    ] {
        let (port, server) = serve(Opts {
            refuse: Some((command, reply)),
            ..Opts::default()
        });
        let smtp = client(port);
        let refusal = smtp.deliver(&mail()).expect_err("nothing is sent");
        assert!(
            refusal.contains(expected),
            "a {command} refusal should carry {expected}: {refusal}"
        );
        drop(server);
    }
}

#[test]
fn a_server_that_hangs_up_is_an_error_and_not_a_hang() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
    let port = listener.local_addr().expect("bound").port();
    thread::spawn(move || {
        let (stream, _) = listener.accept().expect("one connection");
        drop(stream);
    });
    let refusal = client(port).deliver(&mail()).expect_err("nothing is sent");
    assert!(
        refusal.contains("hung up") || refusal.contains("reading"),
        "{refusal}"
    );
}

#[test]
fn a_certificate_from_nobody_in_particular_is_not_trusted() {
    // The same fixture certificate, but the client trusts the Mozilla
    // bundle instead, which is what a real deployment does. This is
    // the assertion that says the verification is on: take it away and
    // any server on the route can read the password.
    let (port, server) = serve(Opts::default());
    let smtp = Smtp {
        user: "postmaster".to_string(),
        pass: "hunter2".to_string(),
        admin_email: "noreply@zou.test".to_string(),
        ..Smtp::new("localhost", port)
    };
    let refusal = smtp.deliver(&mail()).expect_err("nothing is sent");
    assert!(
        refusal.to_lowercase().contains("certificate")
            || refusal.to_lowercase().contains("unknownissuer")
            || refusal.to_lowercase().contains("tls"),
        "{refusal}"
    );
    drop(server);
}
