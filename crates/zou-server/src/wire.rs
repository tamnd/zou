//! The postgres port: one wire listener, every project behind it.
//!
//! A project's database is a real postgres, and the reason it is not
//! simply exposed is that on a node with a thousand of them there is no
//! port to expose it on. So this listens once, reads the startup packet
//! every postgres client sends first, takes the project ref out of it,
//! and proxies the connection to that project's own postmaster.
//!
//! Two spellings of the ref, because there are two kinds of client.
//! `dbname=acme-prod` is what a person types, and `user=postgres.acme-prod`
//! is the convention Supabase's pooler already taught every driver that
//! cannot set a database name freely. The part of the user that is not
//! the ref is the role the session runs as, which is how `anon`,
//! `authenticated` and `service_role` reach SQL as themselves and RLS
//! means the same thing here as it does over http.
//!
//! Authentication is this server's, not the tenant database's. The
//! password is the project key, a JWT signed with the project's secret,
//! exactly the string that goes in an `apikey` header, and its `role`
//! claim has to be the role the connection asked for. That keeps one
//! credential per project instead of two, keeps postgres passwords
//! private to the node that starts the postmaster, and means revoking a
//! key closes this door as well. Verification happens before the attach,
//! so a stranger cannot make this node start a database.
//!
//! Until TLS lands here the key crosses the wire in the clear, so this
//! port belongs on a private network or behind a terminator. An
//! `SSLRequest` is answered with a refusal rather than ignored, which is
//! what tells a client to decide rather than to guess.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use md5::{Digest, Md5};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use zou_store::registry::{Tenant, check_ref};

use crate::attach::Attached;
use crate::tenant::Registry;

/// The magic protocol numbers a startup packet can carry instead of a
/// version. They are version numbers in a range no real version reaches.
const SSL_REQUEST: i32 = 80_877_103;
const GSSENC_REQUEST: i32 = 80_877_104;
const CANCEL_REQUEST: i32 = 80_877_102;
/// 3.0, the only protocol postgres has spoken since 2003.
const PROTOCOL_3: i32 = 196_608;

/// Postgres' own cap on a startup packet, and a good one: everything in
/// there is a name or a setting, so a large one is a client that is
/// confused or a stranger that is not a client at all.
const MAX_STARTUP: usize = 10_000;

/// The cap on any one message read during the login exchange. Password
/// messages are the only thing that arrives here and a project key is
/// hundreds of bytes.
const MAX_LOGIN: usize = 8 << 10;

/// How long a connection has to get from accepted to authenticated. A
/// socket that opens and says nothing is the cheapest thing to hold and
/// the cheapest thing to hold a lot of.
const LOGIN_TIMEOUT: Duration = Duration::from_secs(30);

/// How many live sessions may have a cancel key remembered for them.
/// Past it, cancellation stops working for new sessions and the sessions
/// themselves are unaffected, which is the right way round.
const MAX_CANCEL_KEYS: usize = 16 << 10;

/// The wire front door.
pub struct Wire {
    registry: Arc<Registry>,
    attached: Arc<Attached>,
    /// Which database each live session is on, keyed by the cancel key
    /// that session was handed. A cancel arrives on a new connection
    /// that carries nothing but this pair, so without the map there is
    /// nowhere to send it.
    cancels: Mutex<HashMap<(i32, i32), String>>,
}

impl Wire {
    pub fn new(registry: Arc<Registry>, attached: Arc<Attached>) -> Wire {
        Wire {
            registry,
            attached,
            cancels: Mutex::new(HashMap::new()),
        }
    }

    /// Accept forever. Each connection is a task, and a task that fails
    /// logs and ends, because one client's bad startup packet is not
    /// the listener's problem.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> Result<(), String> {
        loop {
            let (sock, from) = listener
                .accept()
                .await
                .map_err(|e| format!("accept on the pg port: {e}"))?;
            let wire = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(e) = wire.session(sock).await {
                    log::debug!("pg session from {from}: {e}");
                }
            });
        }
    }

    /// One connection, from the startup packet to the last byte.
    pub async fn session(self: Arc<Self>, mut sock: TcpStream) -> Result<(), String> {
        let _live = Live::new();
        let login = tokio::time::timeout(LOGIN_TIMEOUT, self.login(&mut sock)).await;
        let upstream = match login {
            Ok(Ok(Some(upstream))) => upstream,
            // A cancel, which is a whole connection that carries one
            // message and expects no answer at all.
            Ok(Ok(None)) => return Ok(()),
            Ok(Err(stop)) => {
                crate::ops::pg_login(stop.outcome());
                if let Stop::Say { code, message } = &stop {
                    let _ = sock.write_all(&error(code, message)).await;
                }
                let _ = sock.shutdown().await;
                return Err(stop.to_string());
            }
            Err(_) => {
                crate::ops::pg_login("error");
                let _ = sock
                    .write_all(&error("57P05", "no startup packet arrived in time"))
                    .await;
                let _ = sock.shutdown().await;
                return Err("login timed out".to_string());
            }
        };
        crate::ops::pg_login("ok");
        self.pump(sock, upstream).await
    }

    /// Startup to authenticated, answering with the upstream connection
    /// the session runs on. None is a cancel request, which is finished
    /// by the time this returns.
    async fn login(&self, sock: &mut TcpStream) -> Result<Option<TcpStream>, Stop> {
        let params = match hello(sock).await? {
            Hello::Cancel { pid, key } => {
                self.cancel(pid, key).await;
                return Ok(None);
            }
            Hello::Startup(params) => params,
        };
        let route = route(&params)?;
        // The registry before the key check, because verifying against a
        // project's secret needs the secret, and before the attach,
        // because a ref nobody registered must not cost a postmaster.
        let entry = match self.registry.get(&route.tenant).await {
            Ok(Some(entry)) => entry,
            Ok(None) => {
                return Err(Stop::Say {
                    code: "3D000",
                    message: format!("database \"{}\" does not exist", route.tenant),
                });
            }
            Err(e) => {
                log::warn!("registry lookup for {}: {e}", route.tenant);
                return Err(Stop::Say {
                    code: "08006",
                    message: "the project registry could not be read".to_string(),
                });
            }
        };
        authenticate(sock, &entry, &route).await?;

        let dsn = match self.attached.dsn(&entry).await {
            Ok(Some(dsn)) => dsn,
            Ok(None) => {
                return Err(Stop::Say {
                    code: "08006",
                    message: format!("project \"{}\" has no database", route.tenant),
                });
            }
            Err(e) => {
                log::warn!("attach {}: {e}", route.tenant);
                return Err(Stop::Say {
                    code: "08006",
                    message: "the database for this project could not be started".to_string(),
                });
            }
        };
        Ok(Some(connect(&dsn, &params, &route).await?))
    }

    /// Everything after login: relay until the session is ready,
    /// remembering the cancel key on the way past, then get out of the
    /// middle and copy bytes.
    async fn pump(&self, mut sock: TcpStream, mut upstream: TcpStream) -> Result<(), String> {
        let addr = upstream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_default();
        // The AuthenticationOk is this server's, because the exchange
        // the client had was with this server. The database's own one
        // was answered on the other side and ended there.
        sock.write_all(&raw(b'R', &0i32.to_be_bytes()))
            .await
            .map_err(|e| format!("write to the client: {e}"))?;
        let mut key = None;
        loop {
            let (tag, body) = read_message(&mut upstream, 1 << 20)
                .await
                .map_err(|e| e.to_string())?;
            sock.write_all(&raw(tag, &body))
                .await
                .map_err(|e| format!("write to the client: {e}"))?;
            match tag {
                b'K' if body.len() == 8 => {
                    let pid = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                    let secret = i32::from_be_bytes([body[4], body[5], body[6], body[7]]);
                    key = Some((pid, secret));
                }
                // Ready, or refused: either way the login exchange is
                // over and nothing after it is this server's business.
                b'Z' | b'E' => break,
                _ => {}
            }
        }
        if let Some(key) = key {
            let mut cancels = self.cancels.lock().await;
            if cancels.len() < MAX_CANCEL_KEYS {
                cancels.insert(key, addr);
            }
        }
        let moved = tokio::io::copy_bidirectional(&mut sock, &mut upstream).await;
        if let Some(key) = key {
            self.cancels.lock().await.remove(&key);
        }
        match moved {
            Ok((up, down)) => {
                crate::ops::pg_bytes(up, down);
                Ok(())
            }
            // A client that hangs up mid query and a network that broke
            // look the same from here, and neither is worth a warning.
            Err(e) => Err(format!("pg session ended: {e}")),
        }
    }

    /// Send a cancel to the database the session it names is on.
    ///
    /// Passed through rather than translated: the key the client holds
    /// is the key its backend generated, so a session can only be
    /// cancelled by whoever was handed its key, which is exactly the
    /// guarantee postgres itself makes. A pair this node has never seen
    /// is dropped in silence, because the protocol has no reply here and
    /// an attacker guessing keys deserves no signal either.
    async fn cancel(&self, pid: i32, key: i32) {
        let Some(addr) = self.cancels.lock().await.get(&(pid, key)).cloned() else {
            return;
        };
        let mut packet = Vec::with_capacity(16);
        packet.extend_from_slice(&16i32.to_be_bytes());
        packet.extend_from_slice(&CANCEL_REQUEST.to_be_bytes());
        packet.extend_from_slice(&pid.to_be_bytes());
        packet.extend_from_slice(&key.to_be_bytes());
        if let Ok(mut up) = TcpStream::connect(&addr).await {
            let _ = up.write_all(&packet).await;
            let _ = up.shutdown().await;
        }
    }
}

/// What the first packet on a connection turned out to be.
enum Hello {
    Startup(Vec<(String, String)>),
    Cancel { pid: i32, key: i32 },
}

/// Why a connection is not going to happen. `Say` is refused in the
/// client's own protocol, which is what makes psql print a sentence
/// instead of "server closed the connection unexpectedly".
#[derive(Debug)]
enum Stop {
    Say { code: &'static str, message: String },
    Quiet(String),
}

impl Stop {
    fn outcome(&self) -> &'static str {
        match self {
            Stop::Say { .. } => "refused",
            Stop::Quiet(_) => "error",
        }
    }
}

impl std::fmt::Display for Stop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Stop::Say { code, message } => write!(f, "{code}: {message}"),
            Stop::Quiet(why) => write!(f, "{why}"),
        }
    }
}

/// Read startup packets until one of them is a startup packet.
///
/// Encryption requests are the reason this is a loop. Both are declined
/// with a single byte and the client sends its real startup packet
/// after, and a client that asks twice is a client that is not going to
/// stop, so two is the limit.
async fn hello(sock: &mut TcpStream) -> Result<Hello, Stop> {
    for _ in 0..3 {
        let mut head = [0u8; 4];
        sock.read_exact(&mut head)
            .await
            .map_err(|e| Stop::Quiet(format!("no startup packet: {e}")))?;
        let len = i32::from_be_bytes(head) as usize;
        if !(8..=MAX_STARTUP).contains(&len) {
            return Err(Stop::Say {
                code: "08P01",
                message: format!("invalid startup packet length {len}"),
            });
        }
        let mut body = vec![0u8; len - 4];
        sock.read_exact(&mut body)
            .await
            .map_err(|e| Stop::Quiet(format!("short startup packet: {e}")))?;
        let code = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
        match code {
            SSL_REQUEST | GSSENC_REQUEST => {
                sock.write_all(b"N")
                    .await
                    .map_err(|e| Stop::Quiet(format!("declining encryption: {e}")))?;
            }
            CANCEL_REQUEST if body.len() == 12 => {
                return Ok(Hello::Cancel {
                    pid: i32::from_be_bytes([body[4], body[5], body[6], body[7]]),
                    key: i32::from_be_bytes([body[8], body[9], body[10], body[11]]),
                });
            }
            PROTOCOL_3 => return Ok(Hello::Startup(params(&body[4..])?)),
            other => {
                return Err(Stop::Say {
                    code: "0A000",
                    message: format!(
                        "unsupported frontend protocol {}.{}: server supports 3.0",
                        other >> 16,
                        other & 0xffff
                    ),
                });
            }
        }
    }
    Err(Stop::Quiet(
        "asked for encryption too many times".to_string(),
    ))
}

/// The key value pairs of a startup packet: nul terminated strings in
/// pairs, ended by an empty key.
fn params(body: &[u8]) -> Result<Vec<(String, String)>, Stop> {
    let bad = || Stop::Say {
        code: "08P01",
        message: "invalid startup packet layout".to_string(),
    };
    let mut out = Vec::new();
    let mut rest = body;
    loop {
        let (key, tail) = cstr(rest).ok_or_else(bad)?;
        if key.is_empty() {
            return Ok(out);
        }
        let (value, tail) = cstr(tail).ok_or_else(bad)?;
        out.push((key.to_string(), value.to_string()));
        rest = tail;
    }
}

fn cstr(bytes: &[u8]) -> Option<(&str, &[u8])> {
    let end = bytes.iter().position(|b| *b == 0)?;
    let text = std::str::from_utf8(&bytes[..end]).ok()?;
    Some((text, &bytes[end + 1..]))
}

fn param<'a>(params: &'a [(String, String)], name: &str) -> Option<&'a str> {
    params
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

/// Which project, and as whom.
struct Route {
    tenant: String,
    role: String,
    /// What the client called itself, for the message it gets when the
    /// key does not match, since that is the string it has to fix.
    user: String,
}

/// Read the ref out of the startup parameters.
///
/// The user suffix wins over the database name, because a client that
/// spelled the ref in both places was told to by a driver that could
/// not set the database freely, and the suffix is the one it meant.
fn route(params: &[(String, String)]) -> Result<Route, Stop> {
    if param(params, "replication").is_some_and(|v| !matches!(v, "false" | "0" | "off")) {
        return Err(Stop::Say {
            code: "0A000",
            message: "replication connections are not routed through this port".to_string(),
        });
    }
    let user = param(params, "user").unwrap_or_default().to_string();
    if user.is_empty() {
        return Err(Stop::Say {
            code: "08P01",
            message: "no user in the startup packet".to_string(),
        });
    }
    let database = param(params, "database").unwrap_or(&user);
    let (role, tenant) = match user.rsplit_once('.') {
        Some((role, tenant)) if check_ref(tenant).is_ok() => (role, tenant),
        _ => (user.as_str(), database),
    };
    if check_ref(tenant).is_err() {
        return Err(Stop::Say {
            code: "3D000",
            message: format!("database \"{tenant}\" does not exist"),
        });
    }
    // The role goes into the connection this server opens next, so it
    // has to be a name and not a sentence with a space in it.
    if role.is_empty()
        || role.len() > 63
        || !role.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        return Err(Stop::Say {
            code: "28000",
            message: format!("role \"{role}\" is not a name this server will use"),
        });
    }
    Ok(Route {
        tenant: tenant.to_string(),
        role: role.to_string(),
        user,
    })
}

/// Ask for the project key and check it.
///
/// Cleartext, because the password is a JWT and the point is to read
/// it: md5 and SCRAM both prove a shared secret without revealing it,
/// which is the opposite of what a bearer token is for.
async fn authenticate(sock: &mut TcpStream, entry: &Tenant, route: &Route) -> Result<(), Stop> {
    let mut ask = Vec::with_capacity(9);
    ask.push(b'R');
    ask.extend_from_slice(&8i32.to_be_bytes());
    ask.extend_from_slice(&3i32.to_be_bytes());
    sock.write_all(&ask)
        .await
        .map_err(|e| Stop::Quiet(format!("asking for a password: {e}")))?;

    let (tag, body) = read_message(sock, MAX_LOGIN).await?;
    if tag != b'p' {
        return Err(Stop::Say {
            code: "08P01",
            message: format!("expected a password, got message type {}", tag as char),
        });
    }
    let token = body.split(|b| *b == 0).next().unwrap_or_default();
    let token = std::str::from_utf8(token).unwrap_or_default().trim();
    let refused = || Stop::Say {
        code: "28P01",
        message: format!(
            "password authentication failed for user \"{}\": the password is the project's api key",
            route.user
        ),
    };
    let claims = crate::jwt::verify(token, entry.jwt_secret.as_bytes()).map_err(|_| refused())?;
    match claims.role.as_deref() {
        Some(role) if role == route.role => Ok(()),
        Some(role) => Err(Stop::Say {
            code: "28000",
            message: format!(
                "this key is for the role \"{role}\", not \"{}\"",
                route.role
            ),
        }),
        None => Err(refused()),
    }
}

/// Open the session on the tenant's own postgres.
///
/// The startup packet sent on is the client's, with the three things
/// this server decides replaced: the user and database are the ones in
/// the dsn, since the credential for a tenant's postgres belongs to the
/// node that started it, and `options` gains the role the caller proved
/// it may be, which postgres applies before the first statement runs.
async fn connect(dsn: &str, params: &[(String, String)], route: &Route) -> Result<TcpStream, Stop> {
    let cfg: tokio_postgres::Config = dsn.parse().map_err(|e| {
        log::warn!("tenant dsn: {e}");
        Stop::Say {
            code: "08006",
            message: "the database for this project is not addressable".to_string(),
        }
    })?;
    let host = match cfg.get_hosts().first() {
        Some(tokio_postgres::config::Host::Tcp(host)) => host.clone(),
        _ => {
            return Err(Stop::Say {
                code: "08006",
                message: "the database for this project is not on a tcp port".to_string(),
            });
        }
    };
    let port = cfg.get_ports().first().copied().unwrap_or(5432);
    let user = cfg.get_user().unwrap_or("postgres");
    let dbname = cfg.get_dbname().unwrap_or("postgres");

    let mut startup = Vec::with_capacity(128);
    startup.extend_from_slice(&PROTOCOL_3.to_be_bytes());
    let mut put = |key: &str, value: &str| {
        startup.extend_from_slice(key.as_bytes());
        startup.push(0);
        startup.extend_from_slice(value.as_bytes());
        startup.push(0);
    };
    put("user", user);
    put("database", dbname);
    let mut options = param(params, "options").unwrap_or_default().to_string();
    options.push_str(&format!(" -c role={}", route.role));
    put("options", options.trim());
    for (key, value) in params {
        if !matches!(
            key.as_str(),
            "user" | "database" | "options" | "replication"
        ) {
            put(key, value);
        }
    }
    startup.push(0);
    let len =
        i32::try_from(startup.len() + 4).map_err(|_| Stop::Quiet("startup too big".into()))?;

    let mut up = TcpStream::connect((host.as_str(), port))
        .await
        .map_err(|e| {
            log::warn!("dial {host}:{port}: {e}");
            Stop::Say {
                code: "08006",
                message: "the database for this project could not be reached".to_string(),
            }
        })?;
    up.write_all(&len.to_be_bytes())
        .await
        .map_err(|e| Stop::Quiet(format!("startup to the database: {e}")))?;
    up.write_all(&startup)
        .await
        .map_err(|e| Stop::Quiet(format!("startup to the database: {e}")))?;
    handshake(&mut up, cfg.get_password().unwrap_or_default(), user).await?;
    Ok(up)
}

/// Answer whatever the tenant's postgres asks for, and stop at the
/// AuthenticationOk that ends the exchange.
///
/// Trust, cleartext and md5 are what a postmaster this node started
/// asks for. SCRAM is refused with a sentence rather than attempted,
/// because a half implemented SCRAM is a connection that fails in a way
/// nobody can read.
async fn handshake(up: &mut TcpStream, password: &[u8], user: &str) -> Result<(), Stop> {
    loop {
        let (tag, body) = read_message(up, MAX_LOGIN).await?;
        if tag == b'E' {
            return Err(Stop::Say {
                code: "08006",
                message: field(&body, b'M')
                    .unwrap_or_else(|| "the database refused the connection".to_string()),
            });
        }
        if tag != b'R' || body.len() < 4 {
            return Err(Stop::Quiet(format!(
                "unexpected message {} from the database during login",
                tag as char
            )));
        }
        match i32::from_be_bytes([body[0], body[1], body[2], body[3]]) {
            0 => return Ok(()),
            3 => {
                let mut msg = password.to_vec();
                msg.push(0);
                up.write_all(&raw(b'p', &msg))
                    .await
                    .map_err(|e| Stop::Quiet(format!("password to the database: {e}")))?;
            }
            5 if body.len() == 8 => {
                let mut msg = md5_password(password, user.as_bytes(), &body[4..8]).into_bytes();
                msg.push(0);
                up.write_all(&raw(b'p', &msg))
                    .await
                    .map_err(|e| Stop::Quiet(format!("password to the database: {e}")))?;
            }
            10 => {
                return Err(Stop::Say {
                    code: "0A000",
                    message:
                        "this project's database asks for SCRAM, which this port does not speak yet"
                            .to_string(),
                });
            }
            other => {
                return Err(Stop::Quiet(format!(
                    "the database asked for authentication method {other}"
                )));
            }
        }
    }
}

/// Postgres' md5 password: the hex of the digest of the hex of the
/// digest of the password and the user, and the salt, with `md5` in
/// front of it.
fn md5_password(password: &[u8], user: &[u8], salt: &[u8]) -> String {
    let mut first = Md5::new();
    first.update(password);
    first.update(user);
    let inner = hex(&first.finalize());
    let mut second = Md5::new();
    second.update(inner.as_bytes());
    second.update(salt);
    format!("md5{}", hex(&second.finalize()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// One tagged message, with a cap so a length prefix cannot ask this
/// process for a gigabyte.
async fn read_message<R>(r: &mut R, cap: usize) -> Result<(u8, Vec<u8>), Stop>
where
    R: AsyncRead + Unpin,
{
    let mut head = [0u8; 5];
    r.read_exact(&mut head)
        .await
        .map_err(|e| Stop::Quiet(format!("reading a message: {e}")))?;
    let len = i32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
    if len < 4 || len - 4 > cap {
        return Err(Stop::Say {
            code: "08P01",
            message: format!("message of {len} bytes is out of bounds"),
        });
    }
    let mut body = vec![0u8; len - 4];
    r.read_exact(&mut body)
        .await
        .map_err(|e| Stop::Quiet(format!("reading a message body: {e}")))?;
    Ok((head[0], body))
}

/// A tagged message on the wire.
fn raw(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push(tag);
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// A fatal ErrorResponse, which is the only thing this server says in
/// its own voice.
fn error(code: &str, message: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(message.len() + 32);
    for (tag, text) in [
        (b'S', "FATAL"),
        (b'V', "FATAL"),
        (b'C', code),
        (b'M', message),
    ] {
        body.push(tag);
        body.extend_from_slice(text.as_bytes());
        body.push(0);
    }
    body.push(0);
    raw(b'E', &body)
}

/// One field out of an ErrorResponse body.
fn field(body: &[u8], want: u8) -> Option<String> {
    let mut rest = body;
    while let Some((&tag, tail)) = rest.split_first() {
        if tag == 0 {
            return None;
        }
        let (text, tail) = cstr(tail)?;
        if tag == want {
            return Some(text.to_string());
        }
        rest = tail;
    }
    None
}

/// A live session on the gauge, counted down however the session ends.
struct Live;

impl Live {
    fn new() -> Live {
        crate::ops::pg_session(true);
        Live
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        crate::ops::pg_session(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;

    use zou_store::{CasStore, LocalFsStore, registry};

    use crate::attach::Backend;

    const SECRET: &str = "super-secret-jwt-token-with-at-least-32-characters-long";

    /// A postgres that is not one: it reads the startup packet, says
    /// the session is ready, and echoes. Enough to prove what arrived
    /// and that bytes cross in both directions after.
    #[derive(Default)]
    struct Fake {
        startups: StdMutex<Vec<Vec<(String, String)>>>,
        cancels: StdMutex<Vec<(i32, i32)>>,
    }

    impl Fake {
        async fn spawn(self: &Arc<Self>) -> String {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
            let addr = listener.local_addr().expect("an address").to_string();
            let fake = Arc::clone(self);
            tokio::spawn(async move {
                loop {
                    let Ok((sock, _)) = listener.accept().await else {
                        return;
                    };
                    let fake = Arc::clone(&fake);
                    tokio::spawn(async move { fake.one(sock).await });
                }
            });
            addr
        }

        async fn one(&self, mut sock: TcpStream) {
            let mut head = [0u8; 4];
            if sock.read_exact(&mut head).await.is_err() {
                return;
            }
            let len = i32::from_be_bytes(head) as usize;
            let mut body = vec![0u8; len - 4];
            if sock.read_exact(&mut body).await.is_err() {
                return;
            }
            let code = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
            if code == CANCEL_REQUEST {
                self.cancels.lock().unwrap().push((
                    i32::from_be_bytes([body[4], body[5], body[6], body[7]]),
                    i32::from_be_bytes([body[8], body[9], body[10], body[11]]),
                ));
                return;
            }
            self.startups
                .lock()
                .unwrap()
                .push(params(&body[4..]).ok().unwrap_or_default());
            let mut hello = Vec::new();
            hello.extend_from_slice(&raw(b'R', &0i32.to_be_bytes()));
            let mut key = Vec::new();
            key.extend_from_slice(&4242i32.to_be_bytes());
            key.extend_from_slice(&99i32.to_be_bytes());
            hello.extend_from_slice(&raw(b'K', &key));
            hello.extend_from_slice(&raw(b'Z', b"I"));
            if sock.write_all(&hello).await.is_err() {
                return;
            }
            let mut buf = [0u8; 1024];
            while let Ok(n) = sock.read(&mut buf).await {
                if n == 0 || sock.write_all(&buf[..n]).await.is_err() {
                    return;
                }
            }
        }

        fn startup(&self) -> Vec<(String, String)> {
            self.startups
                .lock()
                .unwrap()
                .first()
                .cloned()
                .expect("a startup packet reached the database")
        }
    }

    /// The attach backend: every tenant is the one fake postgres.
    struct Point {
        dsn: String,
        ups: StdMutex<Vec<String>>,
    }

    impl Backend for Point {
        fn up(&self, entry: &Tenant) -> Result<crate::Config, String> {
            self.ups.lock().unwrap().push(entry.tenant_ref.clone());
            Ok(crate::Config {
                jwt_secret: entry.jwt_secret.as_bytes().to_vec(),
                pg: Some(self.dsn.clone()),
                ..crate::Config::default()
            })
        }
        fn down(&self, _tenant_ref: &str) {}
    }

    /// A registry with one project in it, the fake postgres behind it,
    /// and the wire door in front, listening on a real port.
    async fn one_project() -> (tempfile::TempDir, Arc<Fake>, Arc<Point>, String) {
        let dir = tempfile::tempdir().expect("a directory");
        let store: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dir.path()));
        registry::create(&*store, &Tenant::new("acme-prod", SECRET, 1)).expect("a project");

        let fake = Arc::new(Fake::default());
        let addr = fake.spawn().await;
        let (host, port) = addr.rsplit_once(':').expect("host and port");
        let dsn = format!("host={host} port={port} user=zou dbname=postgres");
        let backend = Arc::new(Point {
            dsn,
            ups: StdMutex::new(Vec::new()),
        });
        let wire = Arc::new(Wire::new(
            Arc::new(Registry::new(store)),
            Arc::new(Attached::new(backend.clone())),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
        let addr = listener.local_addr().expect("an address").to_string();
        tokio::spawn(wire.serve(listener));
        (dir, fake, backend, addr)
    }

    fn key(role: &str) -> String {
        crate::jwt::mint(&crate::jwt::key_claims(role), SECRET.as_bytes())
    }

    /// A client: the startup packet, the password, and whatever comes
    /// back.
    struct Client {
        sock: TcpStream,
    }

    impl Client {
        async fn open(addr: &str) -> Client {
            Client {
                sock: TcpStream::connect(addr).await.expect("the wire port"),
            }
        }

        async fn startup(&mut self, params: &[(&str, &str)]) {
            let mut body = Vec::new();
            body.extend_from_slice(&PROTOCOL_3.to_be_bytes());
            for (key, value) in params {
                body.extend_from_slice(key.as_bytes());
                body.push(0);
                body.extend_from_slice(value.as_bytes());
                body.push(0);
            }
            body.push(0);
            self.sock
                .write_all(&((body.len() + 4) as i32).to_be_bytes())
                .await
                .expect("a startup packet");
            self.sock.write_all(&body).await.expect("a startup packet");
        }

        async fn message(&mut self) -> (u8, Vec<u8>) {
            read_message(&mut self.sock, 1 << 20)
                .await
                .expect("a message back")
        }

        /// Startup, answer the password challenge, and return the first
        /// message after: AuthenticationOk when it worked, ErrorResponse
        /// when it did not.
        async fn login(&mut self, params: &[(&str, &str)], password: &str) -> (u8, Vec<u8>) {
            self.startup(params).await;
            let (tag, body) = self.message().await;
            if tag != b'R' {
                return (tag, body);
            }
            let mut msg = password.as_bytes().to_vec();
            msg.push(0);
            self.sock
                .write_all(&raw(b'p', &msg))
                .await
                .expect("a password");
            self.message().await
        }
    }

    fn says(body: &[u8]) -> (String, String) {
        (
            field(body, b'C').unwrap_or_default(),
            field(body, b'M').unwrap_or_default(),
        )
    }

    #[tokio::test]
    async fn the_ref_in_the_user_picks_the_project_and_the_rest_of_it_picks_the_role() {
        let (_d, fake, _backend, addr) = one_project().await;
        let mut client = Client::open(&addr).await;
        let (tag, _) = client
            .login(&[("user", "service_role.acme-prod")], &key("service_role"))
            .await;
        assert_eq!(tag, b'R', "the session should have been let in");
        let startup = fake.startup();
        assert_eq!(
            param(&startup, "user"),
            Some("zou"),
            "the database is reached with the credential the node that started it owns"
        );
        assert_eq!(param(&startup, "database"), Some("postgres"));
        assert_eq!(
            param(&startup, "options"),
            Some("-c role=service_role"),
            "and the role the caller proved is what the session runs as"
        );
    }

    #[tokio::test]
    async fn the_database_name_picks_the_project_too() {
        let (_d, fake, _backend, addr) = one_project().await;
        let mut client = Client::open(&addr).await;
        let (tag, _) = client
            .login(
                &[
                    ("user", "authenticated"),
                    ("database", "acme-prod"),
                    ("application_name", "psql"),
                ],
                &key("authenticated"),
            )
            .await;
        assert_eq!(tag, b'R');
        let startup = fake.startup();
        assert_eq!(param(&startup, "options"), Some("-c role=authenticated"));
        assert_eq!(
            param(&startup, "application_name"),
            Some("psql"),
            "everything this server does not decide is passed through"
        );
    }

    #[tokio::test]
    async fn a_project_nobody_registered_costs_no_database() {
        let (_d, _fake, backend, addr) = one_project().await;
        let mut client = Client::open(&addr).await;
        let (tag, body) = client.login(&[("user", "postgres.nobody")], "").await;
        assert_eq!(tag, b'E');
        let (code, message) = says(&body);
        assert_eq!(code, "3D000");
        assert!(message.contains("nobody"), "{message}");
        assert!(
            backend.ups.lock().unwrap().is_empty(),
            "a stranger must not be able to start a postmaster"
        );
    }

    #[tokio::test]
    async fn the_password_is_the_projects_key_and_nothing_else_is() {
        let (_d, _fake, backend, addr) = one_project().await;
        let elsewhere =
            crate::jwt::mint(&crate::jwt::key_claims("service_role"), b"another project");
        let mut client = Client::open(&addr).await;
        let (tag, body) = client
            .login(&[("user", "service_role.acme-prod")], &elsewhere)
            .await;
        assert_eq!(tag, b'E');
        assert_eq!(says(&body).0, "28P01");
        assert!(
            backend.ups.lock().unwrap().is_empty(),
            "the key is checked before the attach, so a wrong one costs nothing"
        );
    }

    #[tokio::test]
    async fn a_key_for_one_role_cannot_be_used_as_another() {
        let (_d, _fake, _backend, addr) = one_project().await;
        let mut client = Client::open(&addr).await;
        let (tag, body) = client
            .login(&[("user", "service_role.acme-prod")], &key("anon"))
            .await;
        assert_eq!(tag, b'E');
        let (code, message) = says(&body);
        assert_eq!(code, "28000");
        assert!(message.contains("anon"), "{message}");
    }

    #[tokio::test]
    async fn an_encryption_request_is_declined_and_the_startup_after_it_is_served() {
        let (_d, _fake, _backend, addr) = one_project().await;
        let mut client = Client::open(&addr).await;
        for code in [SSL_REQUEST, GSSENC_REQUEST] {
            let mut packet = Vec::new();
            packet.extend_from_slice(&8i32.to_be_bytes());
            packet.extend_from_slice(&code.to_be_bytes());
            client.sock.write_all(&packet).await.expect("a request");
            let mut answer = [0u8; 1];
            client
                .sock
                .read_exact(&mut answer)
                .await
                .expect("an answer");
            assert_eq!(&answer, b"N", "a no is an answer and silence is not");
        }
        let (tag, _) = client
            .login(&[("user", "anon.acme-prod")], &key("anon"))
            .await;
        assert_eq!(tag, b'R');
    }

    #[tokio::test]
    async fn a_protocol_this_server_does_not_speak_is_said_so_in_that_protocol() {
        let (_d, _fake, _backend, addr) = one_project().await;
        let mut client = Client::open(&addr).await;
        let mut packet = Vec::new();
        packet.extend_from_slice(&9i32.to_be_bytes());
        packet.extend_from_slice(&131072i32.to_be_bytes()); // 2.0
        packet.push(0);
        client.sock.write_all(&packet).await.expect("a packet");
        let (tag, body) = client.message().await;
        assert_eq!(tag, b'E');
        let (code, message) = says(&body);
        assert_eq!(code, "0A000");
        assert!(message.contains("2.0"), "{message}");
    }

    #[tokio::test]
    async fn bytes_cross_in_both_directions_once_the_session_is_up() {
        let (_d, _fake, _backend, addr) = one_project().await;
        let mut client = Client::open(&addr).await;
        assert_eq!(
            client
                .login(&[("user", "anon.acme-prod")], &key("anon"))
                .await
                .0,
            b'R'
        );
        // The rest of the login exchange the database sent, relayed.
        let (tag, body) = client.message().await;
        assert_eq!(tag, b'K', "the cancel key has to reach the client");
        assert_eq!(
            i32::from_be_bytes([body[0], body[1], body[2], body[3]]),
            4242
        );
        assert_eq!(client.message().await.0, b'Z');

        let query = raw(b'Q', b"select 1\0");
        client.sock.write_all(&query).await.expect("a query");
        let (tag, body) = client.message().await;
        assert_eq!((tag, body.as_slice()), (b'Q', &b"select 1\0"[..]));
    }

    #[tokio::test]
    async fn a_cancel_reaches_the_database_the_session_is_on() {
        let (_d, fake, _backend, addr) = one_project().await;
        let mut client = Client::open(&addr).await;
        assert_eq!(
            client
                .login(&[("user", "anon.acme-prod")], &key("anon"))
                .await
                .0,
            b'R'
        );
        assert_eq!(client.message().await.0, b'K');
        assert_eq!(client.message().await.0, b'Z');

        let mut cancel = Client::open(&addr).await;
        let mut packet = Vec::new();
        packet.extend_from_slice(&16i32.to_be_bytes());
        packet.extend_from_slice(&CANCEL_REQUEST.to_be_bytes());
        packet.extend_from_slice(&4242i32.to_be_bytes());
        packet.extend_from_slice(&99i32.to_be_bytes());
        cancel.sock.write_all(&packet).await.expect("a cancel");
        for _ in 0..50 {
            if !fake.cancels.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            fake.cancels.lock().unwrap().as_slice(),
            &[(4242, 99)],
            "the key the client holds is the one its backend generated"
        );
    }

    #[tokio::test]
    async fn a_cancel_for_a_session_this_node_never_had_goes_nowhere() {
        let (_d, fake, _backend, addr) = one_project().await;
        let mut cancel = Client::open(&addr).await;
        let mut packet = Vec::new();
        packet.extend_from_slice(&16i32.to_be_bytes());
        packet.extend_from_slice(&CANCEL_REQUEST.to_be_bytes());
        packet.extend_from_slice(&1i32.to_be_bytes());
        packet.extend_from_slice(&2i32.to_be_bytes());
        cancel.sock.write_all(&packet).await.expect("a cancel");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            fake.cancels.lock().unwrap().is_empty(),
            "guessing a pair must not cancel somebody else's query"
        );
    }

    #[test]
    fn the_user_suffix_wins_over_the_database_name() {
        let params = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<Vec<_>>()
        };
        let both = route(&params(&[
            ("user", "service_role.acme-prod"),
            ("database", "postgres"),
        ]))
        .expect("a route");
        assert_eq!(
            (both.tenant.as_str(), both.role.as_str()),
            ("acme-prod", "service_role")
        );

        let named =
            route(&params(&[("user", "postgres"), ("database", "acme-prod")])).expect("a route");
        assert_eq!(
            (named.tenant.as_str(), named.role.as_str()),
            ("acme-prod", "postgres")
        );

        // A dot in the user is always the separator, even when the
        // database name would have routed too. Role names are sql
        // identifiers and never carry one, so there is nothing else it
        // could have meant, and a rule with an exception in it is a
        // rule a driver gets wrong.
        let dotted = route(&params(&[
            ("user", "first.last"),
            ("database", "acme-prod"),
        ]))
        .expect("a route");
        assert_eq!(
            (dotted.tenant.as_str(), dotted.role.as_str()),
            ("last", "first")
        );
    }

    #[test]
    fn a_role_that_is_not_a_name_is_refused_before_it_reaches_a_connection_string() {
        let params = vec![
            ("user".to_string(), "ro le.acme-prod".to_string()),
            ("database".to_string(), "acme-prod".to_string()),
        ];
        let stop = route(&params).err().expect("a refusal");
        assert!(matches!(stop, Stop::Say { code: "28000", .. }), "{stop}");
    }

    #[test]
    fn the_md5_of_a_password_is_postgres_own() {
        // Checked against postgres: md5(md5('secret' || 'zou') || salt).
        let digest = md5_password(b"secret", b"zou", &[1, 2, 3, 4]);
        assert!(digest.starts_with("md5"));
        assert_eq!(digest.len(), 35);
        assert_ne!(
            digest,
            md5_password(b"secret", b"zou", &[4, 3, 2, 1]),
            "a salt that does not change the digest is not a salt"
        );
    }
}
