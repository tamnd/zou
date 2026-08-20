//! The SMTP transport: the sender that actually puts a message on the
//! wire.
//!
//! GoTrue reaches for gomail, which reaches for Go's net/smtp. There is
//! no equivalent in this tree and the protocol is small, so it is
//! written out here: greeting, EHLO, STARTTLS, AUTH, MAIL, RCPT, DATA,
//! QUIT. What is not written out is TLS, which is rustls, and that is
//! the whole reason this file is worth reading carefully rather than
//! being a curiosity.
//!
//! Two rules are stricter than upstream's and both are deliberate.
//!
//! A password is never sent in the clear. If credentials are
//! configured and the connection is not encrypted, this refuses rather
//! than authenticating, unless the server is on the loopback address
//! where nothing reaches a wire. gomail will happily AUTH over a
//! plaintext socket, which turns one misconfigured port into a leaked
//! mail account.
//!
//! The certificate is verified. There is no knob here for skipping
//! that, because a transport that can be told not to check is a
//! transport that ends up not checking.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::ServerName;
use rustls::{ClientConnection, RootCertStore, StreamOwned};

use crate::mail::{Mail, Sender};

/// How the connection is encrypted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Security {
    /// Plain TCP, then STARTTLS when the server offers it. What port
    /// 587 and port 25 want, and GoTrue's default.
    StartTls,
    /// TLS from the first byte, which is what port 465 wants.
    Implicit,
    /// No encryption at all. Only reachable by configuring it, and
    /// only sane for a relay on the loopback address.
    None,
}

/// Everything needed to hand a message to a mail server. The names are
/// GoTrue's environment variables with GOTRUE_SMTP_ taken off.
pub struct Smtp {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub pass: String,
    /// GOTRUE_SMTP_ADMIN_EMAIL, the envelope sender and the From
    /// address. A mail server will not accept a message without one.
    pub admin_email: String,
    /// GOTRUE_SMTP_SENDER_NAME, the display name on the From address.
    pub sender_name: String,
    pub security: Security,
    /// What the server's certificate is checked against. The Mozilla
    /// bundle unless a caller says otherwise, which only a test does.
    pub roots: Arc<RootCertStore>,
    pub timeout: Duration,
}

impl Smtp {
    /// A transport pointed at a host, with everything else at the
    /// defaults GoTrue uses.
    pub fn new(host: &str, port: u16) -> Smtp {
        Smtp {
            host: host.to_string(),
            port,
            user: String::new(),
            pass: String::new(),
            admin_email: String::new(),
            sender_name: String::new(),
            security: match port {
                465 => Security::Implicit,
                _ => Security::StartTls,
            },
            roots: Arc::new(mozilla_roots()),
            timeout: Duration::from_secs(30),
        }
    }

    /// The From header: a display name when there is one, and the
    /// address on its own when there is not.
    fn sender_field(&self) -> String {
        match self.sender_name.is_empty() {
            true => format!("<{}>", self.admin_email),
            false => format!("{} <{}>", encode_word(&self.sender_name), self.admin_email),
        }
    }

    /// Whether a password may be sent on this connection. The loopback
    /// address is allowed in the clear because a local relay is a pipe
    /// to another process on the same machine, which is how a laptop
    /// and a docker compose both run one.
    fn may_authenticate(&self, encrypted: bool) -> bool {
        encrypted || is_loopback(&self.host)
    }
}

/// A transport built from the environment, or None when no host is
/// configured, which is the signal to keep the dev inbox.
///
/// The names are GoTrue's with GOTRUE_ swapped for ZOU_, the same trade
/// the rest of the settings in this tree make.
pub fn from_env() -> Result<Option<Smtp>, String> {
    from_vars(|name| std::env::var(name).ok().filter(|v| !v.is_empty()))
}

/// The same, reading from anywhere. Tests pass a map rather than
/// setting process wide state that another test is reading.
pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Option<Smtp>, String> {
    let Some(host) = var("ZOU_SMTP_HOST") else {
        return Ok(None);
    };
    let port = match var("ZOU_SMTP_PORT") {
        Some(p) => p
            .parse()
            .map_err(|_| format!("ZOU_SMTP_PORT is {p:?}, which is not a port"))?,
        None => 587,
    };
    let mut smtp = Smtp::new(&host, port);
    smtp.user = var("ZOU_SMTP_USER").unwrap_or_default();
    smtp.pass = var("ZOU_SMTP_PASS").unwrap_or_default();
    smtp.sender_name = var("ZOU_SMTP_SENDER_NAME").unwrap_or_default();
    // Without this there is nothing to put in From and no envelope
    // sender, and every message bounces. Better to say so at startup
    // than at the first signup.
    smtp.admin_email = var("ZOU_SMTP_ADMIN_EMAIL").ok_or(
        "ZOU_SMTP_HOST is set but ZOU_SMTP_ADMIN_EMAIL is not, and a message needs a sender",
    )?;
    if let Some(security) = var("ZOU_SMTP_SECURITY") {
        smtp.security = match security.to_ascii_lowercase().as_str() {
            "starttls" => Security::StartTls,
            "tls" | "implicit" => Security::Implicit,
            // For a mail catcher on the loopback address, which is the
            // only kind of server that has no encryption to offer.
            "none" => Security::None,
            other => {
                return Err(format!(
                    "ZOU_SMTP_SECURITY is {other:?}, expected starttls, tls or none"
                ));
            }
        };
    }
    Ok(Some(smtp))
}

/// The Mozilla CA bundle, which is what everything else on the machine
/// trusts too.
fn mozilla_roots() -> RootCertStore {
    RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    }
}

fn is_loopback(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1" | "[::1]")
}

impl Sender for Smtp {
    fn deliver(&self, mail: &Mail) -> Result<(), String> {
        let stream = TcpStream::connect((self.host.as_str(), self.port))
            .map_err(|e| format!("connecting to {}:{}: {e}", self.host, self.port))?;
        stream
            .set_read_timeout(Some(self.timeout))
            .and_then(|()| stream.set_write_timeout(Some(self.timeout)))
            .map_err(|e| format!("setting the timeout: {e}"))?;
        match self.security {
            Security::Implicit => {
                let tls = self.wrap(stream)?;
                self.talk(tls, true, mail)
            }
            _ => self.greet(stream, mail),
        }
    }

    fn describe(&self) -> String {
        format!("smtp to {}:{}", self.host, self.port)
    }
}

impl Smtp {
    /// The plain start: greeting, EHLO, and then STARTTLS if this is
    /// not already encrypted and the server offers it.
    fn greet(&self, stream: TcpStream, mail: &Mail) -> Result<(), String> {
        let mut session = Session::new(stream);
        session.expect(220)?;
        let offered = session.ehlo()?;
        if self.security == Security::None {
            return self.deliver_on(session, false, mail);
        }
        if !offered.iter().any(|c| c == "STARTTLS") {
            return Err(format!(
                "{}:{} offered no starttls, set the security to none to send anyway",
                self.host, self.port
            ));
        }
        session.say("STARTTLS")?;
        session.expect(220)?;
        let tls = self.wrap(session.into_stream())?;
        self.talk(tls, true, mail)
    }

    /// Everything after the connection is encrypted. The EHLO is sent
    /// again because the capability list before STARTTLS cannot be
    /// trusted, which is the whole point of sending it again.
    fn talk(&self, tls: Encrypted, encrypted: bool, mail: &Mail) -> Result<(), String> {
        let mut session = Session::new(tls);
        if self.security == Security::Implicit {
            session.expect(220)?;
        }
        session.ehlo()?;
        self.deliver_on(session, encrypted, mail)
    }

    fn deliver_on<S: Read + Write>(
        &self,
        mut session: Session<S>,
        encrypted: bool,
        mail: &Mail,
    ) -> Result<(), String> {
        if !self.user.is_empty() {
            if !self.may_authenticate(encrypted) {
                return Err(format!(
                    "refusing to send the smtp password to {} in the clear, the server offered no STARTTLS and this is not loopback, so either point ZOU_SMTP_HOST at a server that does or clear ZOU_SMTP_USER to send unauthenticated",
                    self.host
                ));
            }
            session.authenticate(&self.user, &self.pass)?;
        }
        session.say(&format!("MAIL FROM:<{}>", self.admin_email))?;
        session.expect(250)?;
        session.say(&format!("RCPT TO:<{}>", mail.to))?;
        session.expect(250)?;
        session.say("DATA")?;
        session.expect(354)?;
        session.write_all(compose(self, mail).as_bytes())?;
        session.expect(250)?;
        // The message is accepted at this point, so a server that
        // sulks over QUIT has not lost it.
        let _ = session.say("QUIT");
        Ok(())
    }

    fn wrap(&self, stream: TcpStream) -> Result<Encrypted, String> {
        // The provider is named rather than taken from process state,
        // so nothing in the host application can change what this
        // negotiates by installing a default first.
        let config = rustls::ClientConfig::builder_with_provider(
            rustls::crypto::ring::default_provider().into(),
        )
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("tls setup: {e}"))?
        .with_root_certificates(Arc::clone(&self.roots))
        .with_no_client_auth();
        let name = ServerName::try_from(self.host.clone()).map_err(|_| {
            format!(
                "{} is not a name a certificate can be checked against",
                self.host
            )
        })?;
        let connection = ClientConnection::new(Arc::new(config), name)
            .map_err(|e| format!("tls handshake with {}: {e}", self.host))?;
        Ok(StreamOwned::new(connection, stream))
    }
}

type Encrypted = StreamOwned<ClientConnection, TcpStream>;

/// One side of a conversation: lines out, replies in.
struct Session<S: Read + Write> {
    reader: BufReader<S>,
}

impl<S: Read + Write> Session<S> {
    fn new(stream: S) -> Session<S> {
        Session {
            reader: BufReader::new(stream),
        }
    }

    fn into_stream(self) -> S {
        // Anything the server sent early is dropped with the buffer,
        // which is correct: a server that talks before the STARTTLS
        // handshake is talking outside the encrypted session and must
        // not be listened to.
        self.reader.into_inner()
    }

    fn say(&mut self, line: &str) -> Result<(), String> {
        log::trace!("smtp > {line}");
        self.write_all(format!("{line}\r\n").as_bytes())
    }

    fn write_all(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.reader
            .get_mut()
            .write_all(bytes)
            .and_then(|()| self.reader.get_mut().flush())
            .map_err(|e| format!("writing to the mail server: {e}"))
    }

    /// One reply, which may be several lines. Everything after the
    /// code on each line is handed back, which is where the capability
    /// list lives.
    fn reply(&mut self) -> Result<(u16, Vec<String>), String> {
        let mut lines = Vec::new();
        loop {
            let mut line = String::new();
            let read = self
                .reader
                .read_line(&mut line)
                .map_err(|e| format!("reading from the mail server: {e}"))?;
            if read == 0 {
                return Err("the mail server hung up".to_string());
            }
            let line = line.trim_end_matches(['\r', '\n']);
            log::trace!("smtp < {line}");
            if line.len() < 3 {
                return Err(format!("the mail server answered {line:?}"));
            }
            let code: u16 = line[..3]
                .parse()
                .map_err(|_| format!("the mail server answered {line:?}"))?;
            lines.push(line[3..].trim_start_matches(['-', ' ']).to_string());
            // A hyphen after the code means another line is coming.
            if line.as_bytes().get(3) != Some(&b'-') {
                return Ok((code, lines));
            }
        }
    }

    fn expect(&mut self, wanted: u16) -> Result<Vec<String>, String> {
        let (code, lines) = self.reply()?;
        match code == wanted {
            true => Ok(lines),
            false => Err(format!(
                "the mail server answered {code} {}",
                lines.join(" ")
            )),
        }
    }

    /// EHLO, answered with the capability list. HELO is not attempted:
    /// a server too old for EHLO is too old for anything else here.
    fn ehlo(&mut self) -> Result<Vec<String>, String> {
        // gomail says localhost and so does this. The name is only
        // ever used for logging on the far side.
        self.say("EHLO localhost")?;
        let lines = self.expect(250)?;
        Ok(lines
            .iter()
            .skip(1)
            .map(|l| l.trim().to_ascii_uppercase())
            .collect())
    }

    /// AUTH PLAIN, which every server that takes a password takes, in
    /// one round trip.
    fn authenticate(&mut self, user: &str, pass: &str) -> Result<(), String> {
        let mut raw = Vec::new();
        raw.push(0);
        raw.extend_from_slice(user.as_bytes());
        raw.push(0);
        raw.extend_from_slice(pass.as_bytes());
        self.say(&format!("AUTH PLAIN {}", base64(&raw)))?;
        match self.reply()? {
            (235, _) => Ok(()),
            (code, lines) => Err(format!(
                "the mail server refused the login: {code} {}",
                lines.join(" ")
            )),
        }
    }
}

/// The message as it goes down the wire, headers and all.
///
/// The body is base64 with the lines wrapped short, which sidesteps
/// both of the things that go wrong here: a line over 998 bytes, which
/// is not allowed, and a line starting with a dot, which would end the
/// message early.
pub fn compose(smtp: &Smtp, mail: &Mail) -> String {
    let mut out = String::new();
    out.push_str(&format!("From: {}\r\n", smtp.sender_field()));
    out.push_str(&format!("To: <{}>\r\n", mail.to));
    out.push_str(&format!("Subject: {}\r\n", encode_word(&mail.subject)));
    out.push_str(&format!("Date: {}\r\n", rfc2822(mail.at)));
    out.push_str(&format!("Message-ID: <{}>\r\n", message_id(smtp)));
    out.push_str("MIME-Version: 1.0\r\n");
    out.push_str("Content-Type: text/html; charset=UTF-8\r\n");
    out.push_str("Content-Transfer-Encoding: base64\r\n\r\n");
    let encoded = base64(mail.body.as_bytes());
    for chunk in encoded.as_bytes().chunks(76) {
        out.push_str(std::str::from_utf8(chunk).unwrap_or_default());
        out.push_str("\r\n");
    }
    out.push_str(".\r\n");
    out
}

/// A header value, encoded only if it has to be. Anything outside
/// printable ascii goes in as one encoded word, which is what a
/// subject line carrying a name or a language other than English
/// needs.
fn encode_word(value: &str) -> String {
    let plain = value
        .chars()
        .all(|c| c.is_ascii_graphic() || c == ' ')
        // A quoted display name would need escaping, so anything with
        // a quote or a backslash in it goes down the encoded path too.
        && !value.contains(['"', '\\']);
    match plain {
        true => value.to_string(),
        false => format!("=?UTF-8?B?{}?=", base64(value.as_bytes())),
    }
}

fn message_id(smtp: &Smtp) -> String {
    let mut raw = [0u8; 16];
    getrandom::fill(&mut raw).expect("the os rng never fails");
    let hex: String = raw.iter().map(|b| format!("{b:02x}")).collect();
    let domain = smtp
        .admin_email
        .split_once('@')
        .map(|(_, d)| d.to_string())
        .unwrap_or_else(|| smtp.host.clone());
    format!("{hex}@{domain}")
}

fn base64(raw: &[u8]) -> String {
    use base64ct::Encoding;
    base64ct::Base64::encode_string(raw)
}

/// A Date header, which has to be in this exact shape and in English
/// whatever the machine's locale says.
///
/// Everything is UTC, written as +0000. The calendar arithmetic is the
/// usual civil from days: shift the year to start in March so the leap
/// day lands at the end and the month lengths repeat.
pub fn rfc2822(unix: i64) -> String {
    const DAY: i64 = 86_400;
    let days = unix.div_euclid(DAY);
    let secs = unix.rem_euclid(DAY);
    let weekday = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"][days.rem_euclid(7) as usize];
    let (year, month, day) = civil(days);
    let month = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ][month as usize - 1];
    format!(
        "{weekday}, {day:02} {month} {year} {:02}:{:02}:{:02} +0000",
        secs / 3600,
        (secs / 60) % 60,
        secs % 60
    )
}

/// Days since the epoch to a calendar date.
pub(crate) fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = match mp < 10 {
        true => mp + 3,
        false => mp - 9,
    } as u32;
    let year = match month <= 2 {
        true => year + 1,
        false => year,
    };
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn smtp() -> Smtp {
        Smtp {
            admin_email: "noreply@zou.test".to_string(),
            sender_name: "Zou".to_string(),
            ..Smtp::new("mail.zou.test", 587)
        }
    }

    fn mail() -> Mail {
        Mail {
            to: "someone@zou.test".to_string(),
            subject: "Confirm your email address".to_string(),
            body: "<p>hello</p>".to_string(),
            template: crate::mail::CONFIRMATION.to_string(),
            at: 1_700_000_000,
        }
    }

    #[test]
    fn the_port_picks_the_encryption_it_implies() {
        assert_eq!(Smtp::new("mail.zou.test", 465).security, Security::Implicit);
        assert_eq!(Smtp::new("mail.zou.test", 587).security, Security::StartTls);
        assert_eq!(Smtp::new("mail.zou.test", 25).security, Security::StartTls);
    }

    #[test]
    fn a_password_goes_nowhere_in_the_clear_except_to_the_loopback() {
        let far = smtp();
        assert!(!far.may_authenticate(false), "not over a plain socket");
        assert!(far.may_authenticate(true));
        let near = Smtp::new("127.0.0.1", 1025);
        assert!(
            near.may_authenticate(false),
            "a local relay never puts it on a wire"
        );
        assert!(Smtp::new("localhost", 1025).may_authenticate(false));
    }

    #[test]
    fn the_headers_are_the_ones_a_mail_server_insists_on() {
        let out = compose(&smtp(), &mail());
        assert!(out.starts_with("From: Zou <noreply@zou.test>\r\n"), "{out}");
        assert!(out.contains("To: <someone@zou.test>\r\n"), "{out}");
        assert!(
            out.contains("Subject: Confirm your email address\r\n"),
            "{out}"
        );
        assert!(
            out.contains("Date: Tue, 14 Nov 2023 22:13:20 +0000\r\n"),
            "{out}"
        );
        assert!(
            out.contains("@zou.test>\r\n"),
            "the message id is ours: {out}"
        );
        assert!(out.contains("Content-Type: text/html; charset=UTF-8\r\n"));
        assert!(out.ends_with("\r\n.\r\n"), "the body is terminated: {out}");
        let body = out.split("\r\n\r\n").nth(1).expect("a body");
        let encoded: String = body.lines().take_while(|l| *l != ".").collect();
        use base64ct::Encoding;
        let raw = base64ct::Base64::decode_vec(&encoded).expect("valid base64");
        assert_eq!(String::from_utf8(raw).unwrap(), "<p>hello</p>");
    }

    #[test]
    fn a_body_that_would_break_the_wire_format_cannot() {
        // A line of dots and a line longer than the 998 an smtp line
        // is allowed to be. Base64 makes both of them somebody else's
        // problem, and this is the test that says so.
        let mut mail = mail();
        mail.body = format!(".\r\n.\r\n{}", "x".repeat(4000));
        let out = compose(&smtp(), &mail);
        let body = out.split("\r\n\r\n").nth(1).expect("a body");
        for line in body.lines() {
            assert!(line.len() <= 76, "line too long: {}", line.len());
            assert!(line == "." || !line.starts_with('.'), "a bare dot: {line}");
        }
    }

    #[test]
    fn a_subject_in_another_language_is_encoded_and_an_english_one_is_not() {
        assert_eq!(encode_word("Reset your password"), "Reset your password");
        assert_eq!(
            encode_word("Xác nhận địa chỉ email"),
            "=?UTF-8?B?WMOhYyBuaOG6rW4gxJHhu4thIGNo4buJIGVtYWls?="
        );
        // A quote in a display name would have to be escaped, so it
        // takes the encoded path instead of being written raw.
        assert!(encode_word("Zou \"the\" project").starts_with("=?UTF-8?B?"));
    }

    #[test]
    fn the_from_header_survives_a_project_with_no_sender_name() {
        let mut smtp = smtp();
        smtp.sender_name = String::new();
        assert_eq!(smtp.sender_field(), "<noreply@zou.test>");
    }

    fn env(pairs: &[(&str, &str)]) -> Result<Option<Smtp>, String> {
        let map: std::collections::HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        from_vars(|name| map.get(name).cloned())
    }

    /// The complaint, for settings that should not start a server.
    /// Written out because Smtp holds a password and is deliberately
    /// not printable, so expect_err has nothing to print.
    fn refused(pairs: &[(&str, &str)]) -> String {
        match env(pairs) {
            Err(e) => e,
            Ok(_) => panic!("{pairs:?} should not have been accepted"),
        }
    }

    #[test]
    fn no_host_means_no_transport_and_the_dev_inbox_keeps_the_mail() {
        assert!(
            env(&[("ZOU_SMTP_ADMIN_EMAIL", "noreply@zou.test")])
                .expect("no host is not an error")
                .is_none()
        );
    }

    #[test]
    fn a_host_without_an_address_to_send_from_is_refused_at_startup() {
        let refusal = refused(&[("ZOU_SMTP_HOST", "mail.zou.test")]);
        assert!(refusal.contains("ZOU_SMTP_ADMIN_EMAIL"), "{refusal}");
    }

    #[test]
    fn the_environment_is_read_the_way_gotrue_reads_its_own() {
        let smtp = env(&[
            ("ZOU_SMTP_HOST", "mail.zou.test"),
            ("ZOU_SMTP_PORT", "465"),
            ("ZOU_SMTP_USER", "postmaster"),
            ("ZOU_SMTP_PASS", "hunter2"),
            ("ZOU_SMTP_ADMIN_EMAIL", "noreply@zou.test"),
            ("ZOU_SMTP_SENDER_NAME", "Zou"),
        ])
        .expect("a transport")
        .expect("a host was configured");
        assert_eq!(smtp.host, "mail.zou.test");
        assert_eq!(smtp.port, 465);
        assert_eq!(smtp.user, "postmaster");
        assert_eq!(smtp.pass, "hunter2");
        assert_eq!(smtp.sender_field(), "Zou <noreply@zou.test>");
        assert_eq!(
            smtp.security,
            Security::Implicit,
            "the port still decides when nothing overrides it"
        );
        // The port GoTrue defaults to, for a submission server.
        let smtp = env(&[
            ("ZOU_SMTP_HOST", "mail.zou.test"),
            ("ZOU_SMTP_ADMIN_EMAIL", "noreply@zou.test"),
        ])
        .expect("a transport")
        .expect("a host");
        assert_eq!(smtp.port, 587);
    }

    #[test]
    fn the_encryption_can_be_named_when_the_port_says_the_wrong_thing() {
        let catcher = env(&[
            ("ZOU_SMTP_HOST", "127.0.0.1"),
            ("ZOU_SMTP_PORT", "1025"),
            ("ZOU_SMTP_ADMIN_EMAIL", "noreply@zou.test"),
            ("ZOU_SMTP_SECURITY", "none"),
        ])
        .expect("a transport")
        .expect("a host");
        assert_eq!(catcher.security, Security::None);
        let refusal = refused(&[
            ("ZOU_SMTP_HOST", "mail.zou.test"),
            ("ZOU_SMTP_ADMIN_EMAIL", "noreply@zou.test"),
            ("ZOU_SMTP_SECURITY", "off"),
        ]);
        assert!(refusal.contains("starttls, tls or none"), "{refusal}");
    }

    #[test]
    fn the_date_is_the_one_the_rest_of_the_world_agrees_on() {
        assert_eq!(rfc2822(0), "Thu, 01 Jan 1970 00:00:00 +0000");
        assert_eq!(rfc2822(1_700_000_000), "Tue, 14 Nov 2023 22:13:20 +0000");
        // A leap day, which is where this arithmetic goes wrong when
        // it goes wrong.
        assert_eq!(rfc2822(1_709_164_800), "Thu, 29 Feb 2024 00:00:00 +0000");
        assert_eq!(rfc2822(4_102_444_800), "Fri, 01 Jan 2100 00:00:00 +0000");
    }
}
