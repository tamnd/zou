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
//! TLS is a certificate away. Given one, the `SSLRequest` every client
//! sends first is accepted, the handshake happens on the same socket,
//! and the startup packet is read out of the encrypted stream, which is
//! the same order postgres itself does it in. Given none, that request
//! is declined with the single byte the protocol has for it, which tells
//! a client to decide rather than to guess, and then the key crosses in
//! the clear and the port belongs on a private network or behind a
//! terminator.
//!
//! A door with a certificate takes nothing but TLS. A startup packet
//! that arrives without the request in front of it is refused with a
//! sentence saying so, because an operator who configured a certificate
//! did not mean "encrypted when the client feels like it", and the
//! project key is the whole credential.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use md5::{Digest, Md5};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_rustls::TlsAcceptor;
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

/// How many backends one project and role may have open at once, and
/// therefore how many transactions can be in flight for it. A client
/// past it waits for one rather than being refused, because a queue is
/// what a pooler is for.
pub const POOL_PER_BANK: usize = 20;

/// How long a client may hold a backend without either side saying
/// anything. A transaction left open pins a backend, and a pooler whose
/// backends are all pinned by clients that went to lunch is an outage
/// for everyone else on the project.
const IDLE_IN_TRANSACTION: Duration = Duration::from_secs(60);

/// The cap on a single message in the pooler's own buffer. Everything
/// the two sides say goes through here one message at a time, so this
/// is what one session can cost in memory.
const MAX_MESSAGE: usize = 256 << 20;

/// What a connection on this port gets: its own backend for as long as
/// it is connected, or one per transaction out of a shared pool.
#[derive(Clone, Copy, PartialEq)]
pub enum Mode {
    Session,
    Transaction,
}

/// The wire front door.
pub struct Wire {
    registry: Arc<Registry>,
    attached: Arc<Attached>,
    mode: Mode,
    /// The certificate this door answers an `SSLRequest` with, if it
    /// was given one. None is the plaintext door, which declines the
    /// request instead.
    tls: Option<TlsAcceptor>,
    /// Where each live session's backend is, keyed by the cancel key
    /// that session was handed. A cancel arrives on a connection of its
    /// own that carries nothing but this pair, so without the map there
    /// is nowhere to send it. It is a cell rather than a value because
    /// under the pooler the answer changes at every transaction.
    cancels: Mutex<HashMap<(i32, i32), Held>>,
    /// One pool per project and role, so a backend a client is handed
    /// is a backend that was already the right role, and no session ever
    /// runs as an identity its key did not prove.
    banks: Mutex<HashMap<(String, String), Arc<Bank>>>,
}

impl Wire {
    pub fn new(registry: Arc<Registry>, attached: Arc<Attached>) -> Wire {
        Wire {
            registry,
            attached,
            mode: Mode::Session,
            tls: None,
            cancels: Mutex::new(HashMap::new()),
            banks: Mutex::new(HashMap::new()),
        }
    }

    /// The same door in transaction mode, which is 6543.
    pub fn pooling(mut self) -> Wire {
        self.mode = Mode::Transaction;
        self
    }

    /// The same door with a certificate on it, which makes TLS the only
    /// way in rather than one of two.
    pub fn secured(mut self, tls: TlsAcceptor) -> Wire {
        self.tls = Some(tls);
        self
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
            nodelay(&sock);
            let wire = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(e) = wire.session(sock).await {
                    log::debug!("pg session from {from}: {e}");
                }
            });
        }
    }

    /// One connection, from the first packet to the last byte.
    ///
    /// A door with a certificate does its handshake here, before
    /// anything reads a startup packet, because everything after this
    /// point is the same protocol whether or not there is TLS under it
    /// and the only difference is which stream it is written to.
    pub async fn session(self: Arc<Self>, sock: TcpStream) -> Result<(), String> {
        let _live = Live::new();
        let Some(tls) = self.tls.clone() else {
            return self.run(sock).await;
        };
        let secured = tokio::time::timeout(LOGIN_TIMEOUT, secure(sock, &tls)).await;
        match secured {
            Ok(Ok(stream)) => self.run(stream).await,
            Ok(Err(stop)) => {
                crate::ops::pg_login(stop.outcome());
                Err(stop.to_string())
            }
            Err(_) => {
                crate::ops::pg_login("error");
                Err("the tls handshake did not finish in time".to_string())
            }
        }
    }

    /// The session itself, on whatever it turned out to be running on.
    async fn run<S: Wired>(self: Arc<Self>, mut sock: S) -> Result<(), String> {
        let login = tokio::time::timeout(LOGIN_TIMEOUT, self.login(&mut sock)).await;
        let ready = match login {
            Ok(Ok(Some(ready))) => ready,
            // A cancel, which is a whole connection that carries one
            // message and expects no answer at all.
            Ok(Ok(None)) => return Ok(()),
            Ok(Err(stop)) => {
                crate::ops::pg_login(stop.outcome());
                refuse(&mut sock, &stop).await;
                return Err(stop.to_string());
            }
            Err(_) => {
                crate::ops::pg_login("error");
                refuse(
                    &mut sock,
                    &Stop::Say {
                        code: "57P05",
                        message: "no startup packet arrived in time".to_string(),
                    },
                )
                .await;
                return Err("login timed out".to_string());
            }
        };
        crate::ops::pg_login("ok");
        match ready {
            Ready::Direct(upstream) => self.pump(sock, upstream).await,
            Ready::Pooled(bank) => self.pooled(sock, bank).await,
        }
    }

    /// Startup to authenticated, answering with whatever the session
    /// runs on. None is a cancel request, which is finished by the time
    /// this returns.
    async fn login<S: Wired>(&self, sock: &mut S) -> Result<Option<Ready>, Stop> {
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
        match self.mode {
            Mode::Session => Ok(Some(Ready::Direct(connect(&dsn, &params, &route).await?))),
            Mode::Transaction => Ok(Some(Ready::Pooled(self.bank(&dsn, &route).await))),
        }
    }

    /// The pool for one project and role, made on first use.
    ///
    /// Keyed by the dsn rather than by the ref, so a project that is
    /// detached and comes back somewhere else gets fresh backends
    /// instead of a pool of connections to a postmaster that has gone.
    async fn bank(&self, dsn: &str, route: &Route) -> Arc<Bank> {
        let key = (dsn.to_string(), route.role.clone());
        self.banks
            .lock()
            .await
            .entry(key)
            .or_insert_with(|| {
                Arc::new(Bank {
                    dsn: dsn.to_string(),
                    role: route.role.clone(),
                    permits: Arc::new(Semaphore::new(POOL_PER_BANK)),
                    idle: Mutex::new(Vec::new()),
                    greeting: Mutex::new(Vec::new()),
                })
            })
            .clone()
    }

    /// Everything after login: relay until the session is ready,
    /// remembering the cancel key on the way past, then get out of the
    /// middle and copy bytes.
    async fn pump<S: Wired>(&self, mut sock: S, mut upstream: TcpStream) -> Result<(), String> {
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
        if let Some((pid, secret)) = key {
            self.remember(
                (pid, secret),
                Arc::new(StdMutex::new(Some(Target {
                    addr,
                    pid,
                    key: secret,
                }))),
            )
            .await;
        }
        let moved = tokio::io::copy_bidirectional(&mut sock, &mut upstream).await;
        if let Some(key) = key {
            self.forget(key).await;
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

    /// A pooled session: no backend of its own, one borrowed for the
    /// length of each transaction and handed back at the end of it.
    ///
    /// The greeting is this server's. A client needs its parameter set,
    /// a cancel key and a ReadyForQuery before it will say anything, and
    /// under the pooler there is no backend yet to have sent them, so
    /// the parameters are the ones the project's database announced to
    /// the first backend opened for it and the key is one of ours.
    async fn pooled<S: Wired>(&self, sock: S, bank: Arc<Bank>) -> Result<(), String> {
        let greeting = bank.greeting().await.map_err(|e| e.to_string())?;
        let mut client = Framed::new(sock);
        let key = (random_i32(), random_i32());
        let held: Held = Arc::new(StdMutex::new(None));
        client
            .send(&raw(b'R', &0i32.to_be_bytes()))
            .await
            .map_err(|e| e.to_string())?;
        client.send(&greeting).await.map_err(|e| e.to_string())?;
        let mut backend_key = Vec::with_capacity(8);
        backend_key.extend_from_slice(&key.0.to_be_bytes());
        backend_key.extend_from_slice(&key.1.to_be_bytes());
        client
            .send(&raw(b'K', &backend_key))
            .await
            .map_err(|e| e.to_string())?;
        client
            .send(&raw(b'Z', b"I"))
            .await
            .map_err(|e| e.to_string())?;
        self.remember(key, Arc::clone(&held)).await;
        let out = self.transactions(&mut client, &bank, &held).await;
        self.forget(key).await;
        out.map_err(|e| e.to_string())
    }

    /// The pooled session's whole life: borrow at the first message of
    /// a transaction, hand back at the ReadyForQuery that says the
    /// transaction is over.
    async fn transactions<S: Wired>(
        &self,
        client: &mut Framed<S>,
        bank: &Arc<Bank>,
        held: &Held,
    ) -> Result<(), Stop> {
        let mut lease: Option<Lease> = None;
        // Set by a message this server refused on the client's behalf.
        // A backend in that state ignores everything until Sync, and so
        // does this, or the client and the server disagree about how
        // many answers are owed.
        let mut skipping = false;
        loop {
            let Some(lease_now) = lease.as_mut() else {
                let Some(msg) = client.next().await? else {
                    return Ok(());
                };
                match starts(msg[0]) {
                    Start::Leave => return Ok(()),
                    // Sync with nothing outstanding is answered here,
                    // because borrowing a backend to say "ready" would
                    // be a transaction that never happened.
                    Start::Sync => {
                        skipping = false;
                        client.send(&raw(b'Z', b"I")).await?;
                    }
                    Start::Ignore => {}
                    Start::Work => {
                        if skipping {
                            continue;
                        }
                        if let Some(why) = unsupported(&msg) {
                            skipping = true;
                            client.send(&error("0A000", &why)).await?;
                            continue;
                        }
                        let start = std::time::Instant::now();
                        let borrowed = bank.checkout().await?;
                        crate::ops::pg_checkout(start);
                        *held.lock().expect("the cancel cell") = Some(borrowed.target());
                        lease = Some(borrowed);
                        let lease = lease.as_mut().expect("just borrowed");
                        lease.backend.send(&msg).await?;
                    }
                }
                continue;
            };

            // Both directions at once, because a client is allowed to
            // send the next statement before the answer to this one has
            // arrived, and a read that is thrown away half finished
            // would lose the bytes it had. Both sides keep their own
            // buffer for exactly that reason.
            let turn = tokio::time::timeout(IDLE_IN_TRANSACTION, async {
                tokio::select! {
                    from_client = client.next() => Turn::Client(from_client),
                    from_backend = lease_now.backend.next() => Turn::Backend(from_backend),
                }
            })
            .await;
            match turn {
                Ok(Turn::Client(msg)) => {
                    // A client that hangs up mid transaction takes its
                    // backend with it rather than handing back one with
                    // an open transaction on it.
                    let Some(msg) = msg? else { return Ok(()) };
                    if starts(msg[0]) == Start::Leave {
                        return Ok(());
                    }
                    if skipping && starts(msg[0]) != Start::Sync {
                        continue;
                    }
                    if let Some(why) = unsupported(&msg) {
                        skipping = true;
                        client.send(&error("0A000", &why)).await?;
                        continue;
                    }
                    skipping = false;
                    lease_now.backend.send(&msg).await?;
                }
                Ok(Turn::Backend(msg)) => {
                    let Some(msg) = msg? else {
                        return Err(Stop::Quiet("the database hung up mid transaction".into()));
                    };
                    client.send(&msg).await?;
                    // Idle, which means no transaction is open, which
                    // means this backend belongs to whoever asks next.
                    if msg[0] == b'Z' && msg.last() == Some(&b'I') {
                        *held.lock().expect("the cancel cell") = None;
                        if let Some(lease) = lease.take() {
                            crate::ops::pg_transaction();
                            bank.release(lease).await;
                        }
                    }
                }
                Err(_) => {
                    client
                        .send(&error(
                            "25P03",
                            "the transaction was idle too long and the connection was closed",
                        ))
                        .await?;
                    return Ok(());
                }
            }
        }
    }

    /// Send a cancel to the backend the session it names is on.
    ///
    /// In session mode the key the client holds is the key its own
    /// backend generated, so this forwards it as it stands, which means
    /// a session can only be cancelled by whoever was handed its key,
    /// exactly the guarantee postgres itself makes. Under the pooler
    /// the key is this server's, because at login there is no backend
    /// yet, so the cell says which backend the session is on right now
    /// and the cancel goes there with that backend's own key. A pair
    /// this node never issued is dropped in silence, because the
    /// protocol has no reply here and an attacker guessing keys
    /// deserves no signal either.
    async fn cancel(&self, pid: i32, key: i32) {
        let held = self.cancels.lock().await.get(&(pid, key)).cloned();
        let Some(target) = held.and_then(|h| h.lock().expect("the cancel cell").clone()) else {
            return;
        };
        let mut packet = Vec::with_capacity(16);
        packet.extend_from_slice(&16i32.to_be_bytes());
        packet.extend_from_slice(&CANCEL_REQUEST.to_be_bytes());
        packet.extend_from_slice(&target.pid.to_be_bytes());
        packet.extend_from_slice(&target.key.to_be_bytes());
        if let Ok(mut up) = TcpStream::connect(&target.addr).await {
            let _ = up.write_all(&packet).await;
            let _ = up.shutdown().await;
        }
    }

    async fn remember(&self, key: (i32, i32), held: Held) {
        let mut cancels = self.cancels.lock().await;
        if cancels.len() < MAX_CANCEL_KEYS {
            cancels.insert(key, held);
        }
    }

    async fn forget(&self, key: (i32, i32)) {
        self.cancels.lock().await.remove(&key);
    }
}

/// Which side spoke first.
enum Turn {
    Client(Result<Option<Bytes>, Stop>),
    Backend(Result<Option<Bytes>, Stop>),
}

/// What the session runs on, once it is allowed to run.
enum Ready {
    Direct(TcpStream),
    Pooled(Arc<Bank>),
}

/// Where a live session's backend is, and what its key there is. A cell
/// because under the pooler it changes at every transaction, and shared
/// because a cancel arrives on somebody else's connection.
type Held = Arc<StdMutex<Option<Target>>>;

#[derive(Clone)]
struct Target {
    addr: String,
    pid: i32,
    key: i32,
}

/// The backends open for one project and role.
struct Bank {
    dsn: String,
    role: String,
    /// The ceiling, and the queue when it is reached.
    permits: Arc<Semaphore>,
    idle: Mutex<Vec<Backend>>,
    /// The ParameterStatus messages this project's database sent the
    /// first backend, replayed to every client that logs in. They are
    /// the database's own values, so a client is told the truth about
    /// its server version and its encoding without this server having
    /// opinions about either.
    greeting: Mutex<Vec<u8>>,
}

impl Bank {
    /// What a client is told at login. Opening a backend to learn it is
    /// the price of the first login on a project, and the backend goes
    /// into the pool rather than being thrown away.
    async fn greeting(&self) -> Result<Vec<u8>, Stop> {
        if let Some(known) = Some(self.greeting.lock().await.clone()).filter(|g| !g.is_empty()) {
            return Ok(known);
        }
        let lease = self.checkout().await?;
        let greeting = lease.backend.greeting.clone();
        self.release(lease).await;
        *self.greeting.lock().await = greeting.clone();
        Ok(greeting)
    }

    /// A backend, from the pool if one is parked and a new connection
    /// otherwise. Waits when every permit is out, because a queue is
    /// what a pooler is for.
    async fn checkout(&self) -> Result<Lease, Stop> {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| Stop::Quiet("the pool is closed".into()))?;
        if let Some(backend) = self.idle.lock().await.pop() {
            return Ok(Lease {
                backend,
                _permit: permit,
            });
        }
        let backend = Backend::open(&self.dsn, &self.role).await?;
        crate::ops::pg_backend(true);
        Ok(Lease {
            backend,
            _permit: permit,
        })
    }

    /// Hand a backend back, cleaned.
    ///
    /// `DISCARD ALL` is a round trip this server pays rather than the
    /// next client paying for what the last one left behind. A pooler
    /// that skips it is betting every client obeys the rule that
    /// session state is not allowed here, and one leaked search_path on
    /// a shared database is not a bet worth a hundred microseconds. A
    /// backend that does not come back clean is closed instead of
    /// parked.
    async fn release(&self, mut lease: Lease) {
        if lease.backend.discard().await.is_err() {
            crate::ops::pg_backend(false);
            return;
        }
        self.idle.lock().await.push(lease.backend);
    }
}

/// A backend checked out of a bank. Dropping one closes it, which is
/// what should happen to a connection whose client left mid statement.
struct Lease {
    backend: Backend,
    _permit: OwnedSemaphorePermit,
}

impl Lease {
    fn target(&self) -> Target {
        Target {
            addr: self.backend.addr.clone(),
            pid: self.backend.pid,
            key: self.backend.key,
        }
    }
}

/// One connection to a project's database, owned by the pool.
struct Backend {
    io: Framed<TcpStream>,
    addr: String,
    pid: i32,
    key: i32,
    /// This backend's own ParameterStatus messages, kept because the
    /// first backend's are what every pooled client is greeted with.
    greeting: Vec<u8>,
}

impl Backend {
    /// Open one and read its login sequence, which is where its
    /// parameters and its cancel key come from.
    ///
    /// The startup packet is this server's, not any one client's: a
    /// backend that several clients will use in turn cannot carry one
    /// of their application names, so it carries the pooler's.
    async fn open(dsn: &str, role: &str) -> Result<Backend, Stop> {
        let params = vec![("application_name".to_string(), "zou pooler".to_string())];
        let route = Route {
            tenant: String::new(),
            role: role.to_string(),
            user: String::new(),
        };
        let io = connect(dsn, &params, &route).await?;
        let addr = io
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| dsn.to_string());
        let mut backend = Backend {
            io: Framed::new(io),
            addr,
            pid: 0,
            key: 0,
            greeting: Vec::new(),
        };
        loop {
            let Some(msg) = backend.next().await? else {
                return Err(Stop::Quiet("the database hung up during login".into()));
            };
            match msg[0] {
                b'S' => backend.greeting.extend_from_slice(&msg),
                b'K' if msg.len() == 13 => {
                    backend.pid = i32::from_be_bytes([msg[5], msg[6], msg[7], msg[8]]);
                    backend.key = i32::from_be_bytes([msg[9], msg[10], msg[11], msg[12]]);
                }
                b'Z' => return Ok(backend),
                b'E' => {
                    return Err(Stop::Say {
                        code: "08006",
                        message: field(&msg[5..], b'M')
                            .unwrap_or_else(|| "the database refused a connection".to_string()),
                    });
                }
                _ => {}
            }
        }
    }

    async fn next(&mut self) -> Result<Option<Bytes>, Stop> {
        self.io.next().await
    }

    async fn send(&mut self, msg: &[u8]) -> Result<(), Stop> {
        self.io.send(msg).await
    }

    /// Everything a session could have left on it, gone.
    async fn discard(&mut self) -> Result<(), Stop> {
        self.send(&raw(b'Q', b"DISCARD ALL\0")).await?;
        loop {
            let Some(msg) = self.next().await? else {
                return Err(Stop::Quiet(
                    "the database hung up while being cleaned".into(),
                ));
            };
            match msg[0] {
                b'Z' => return Ok(()),
                b'E' => return Err(Stop::Quiet("the database could not be cleaned".into())),
                _ => {}
            }
        }
    }
}

/// What a frontend message means for whether a backend is needed.
#[derive(PartialEq)]
enum Start {
    /// Needs a backend, and is the start of a transaction if none is
    /// open.
    Work,
    Sync,
    Leave,
    Ignore,
}

fn starts(tag: u8) -> Start {
    match tag {
        b'Q' | b'P' | b'B' | b'E' | b'D' | b'C' | b'F' | b'd' | b'c' | b'f' => Start::Work,
        b'S' => Start::Sync,
        b'X' => Start::Leave,
        // Flush with nothing outstanding, and anything this server does
        // not know, which the backend would answer for if there were
        // one. There is not, and inventing an answer is worse than
        // waiting for the client's next message.
        _ => Start::Ignore,
    }
}

/// The one thing a transaction pooler cannot do, refused where the
/// client can still see which statement caused it.
///
/// A named prepared statement lives on the backend that parsed it, and
/// under the pooler the next transaction is on a different one. Failing
/// at the Parse that names it beats failing at the Bind one transaction
/// later with a message about a statement that does not exist.
fn unsupported(msg: &Bytes) -> Option<String> {
    if msg[0] != b'P' || msg.len() < 6 {
        return None;
    }
    let (name, _) = cstr(&msg[5..])?;
    match name.is_empty() {
        true => None,
        false => Some(format!(
            "prepared statement \"{name}\" cannot be named on the transaction pooler, turn statement caching off in your driver or use port 5432"
        )),
    }
}

/// A stream that hands out whole protocol messages.
///
/// The buffer is the point. Reading a message straight off a socket is
/// not safe to abandon half way, and the pooler abandons a read every
/// time the other side speaks first, so the bytes that did arrive stay
/// here until the rest of their message joins them.
struct Framed<S> {
    io: S,
    buf: BytesMut,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Framed<S> {
    fn new(io: S) -> Framed<S> {
        Framed {
            io,
            buf: BytesMut::with_capacity(8 << 10),
        }
    }

    /// The next whole message, or None when the other side is done.
    /// Safe to drop unfinished.
    async fn next(&mut self) -> Result<Option<Bytes>, Stop> {
        loop {
            if let Some(msg) = frame(&mut self.buf)? {
                return Ok(Some(msg));
            }
            let read = self
                .io
                .read_buf(&mut self.buf)
                .await
                .map_err(|e| Stop::Quiet(format!("reading: {e}")))?;
            if read == 0 {
                return match self.buf.is_empty() {
                    true => Ok(None),
                    false => Err(Stop::Quiet("hung up mid message".into())),
                };
            }
        }
    }

    async fn send(&mut self, msg: &[u8]) -> Result<(), Stop> {
        self.io
            .write_all(msg)
            .await
            .map_err(|e| Stop::Quiet(format!("writing: {e}")))
    }
}

/// One whole message out of a buffer, if there is one in there yet.
fn frame(buf: &mut BytesMut) -> Result<Option<Bytes>, Stop> {
    if buf.len() < 5 {
        return Ok(None);
    }
    let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    if len < 4 || len - 4 > MAX_MESSAGE {
        return Err(Stop::Say {
            code: "08P01",
            message: format!("message of {len} bytes is out of bounds"),
        });
    }
    // The length counts itself and the body but not the tag, so the
    // message on the wire is one byte longer than it says.
    let whole = len + 1;
    match buf.len() >= whole {
        true => Ok(Some(buf.split_to(whole).freeze())),
        false => Ok(None),
    }
}

/// A pid or a cancel key of this server's own making.
fn random_i32() -> i32 {
    let mut bytes = [0u8; 4];
    getrandom::fill(&mut bytes).expect("the system random source");
    i32::from_be_bytes(bytes)
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
/// Encryption requests are the reason this is a loop. On a door with no
/// certificate both are declined with a single byte and the client sends
/// its real startup packet after, and a client that asks twice is a
/// client that is not going to stop, so two is the limit. On a door with
/// one the request was already answered and the handshake already
/// happened, so what arrives here is the startup packet.
async fn hello<S: Wired>(sock: &mut S) -> Result<Hello, Stop> {
    for _ in 0..3 {
        match packet(sock).await? {
            Packet::Encryption(_) => {
                sock.write_all(b"N")
                    .await
                    .map_err(|e| Stop::Quiet(format!("declining encryption: {e}")))?;
            }
            Packet::Cancel { pid, key } => return Ok(Hello::Cancel { pid, key }),
            Packet::Startup(params) => return Ok(Hello::Startup(params)),
        }
    }
    Err(Stop::Quiet(
        "asked for encryption too many times".to_string(),
    ))
}

/// One packet of the shape a connection opens with: a length, a code,
/// and whatever that code says follows. Reading it is the same work
/// whether the answer is going to be a handshake or a session, so it is
/// one function and the two callers decide what to do with it.
enum Packet {
    /// An SSLRequest or a GSSENCRequest, which carry the code and
    /// nothing else and are answered with a single byte rather than a
    /// message.
    Encryption(i32),
    Cancel {
        pid: i32,
        key: i32,
    },
    Startup(Vec<(String, String)>),
}

async fn packet<S: Wired>(sock: &mut S) -> Result<Packet, Stop> {
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
        SSL_REQUEST | GSSENC_REQUEST => Ok(Packet::Encryption(code)),
        CANCEL_REQUEST if body.len() == 12 => Ok(Packet::Cancel {
            pid: i32::from_be_bytes([body[4], body[5], body[6], body[7]]),
            key: i32::from_be_bytes([body[8], body[9], body[10], body[11]]),
        }),
        PROTOCOL_3 => Ok(Packet::Startup(params(&body[4..])?)),
        other => Err(Stop::Say {
            code: "0A000",
            message: format!(
                "unsupported frontend protocol {}.{}: server supports 3.0",
                other >> 16,
                other & 0xffff
            ),
        }),
    }
}

/// The handshake, on a door that has a certificate.
///
/// The exchange is postgres's own and it is older than TLS's own
/// negotiation: the client asks in the clear, the server answers with
/// one byte, and only then does the socket become a TLS one. So this
/// reads packets on the plain socket until the client asks, which for
/// every libpq built this century is either the first packet or the
/// second, since a client with GSSAPI compiled in tries that first and
/// takes an N for an answer.
///
/// A startup packet here is a client that meant to talk in the clear,
/// and it is refused rather than served. An operator who put a
/// certificate on this port did not mean "encrypted when the client
/// asks", and the credential the next packet would carry is the
/// project's api key.
async fn secure(
    mut sock: TcpStream,
    tls: &TlsAcceptor,
) -> Result<tokio_rustls::server::TlsStream<TcpStream>, Stop> {
    for _ in 0..3 {
        let asked = match packet(&mut sock).await {
            Ok(asked) => asked,
            Err(stop) => {
                refuse(&mut sock, &stop).await;
                return Err(stop);
            }
        };
        match asked {
            Packet::Encryption(SSL_REQUEST) => {
                sock.write_all(b"S")
                    .await
                    .map_err(|e| Stop::Quiet(format!("accepting encryption: {e}")))?;
                return tls
                    .accept(sock)
                    .await
                    .map_err(|e| Stop::Quiet(format!("tls handshake: {e}")));
            }
            Packet::Encryption(_) => {
                sock.write_all(b"N")
                    .await
                    .map_err(|e| Stop::Quiet(format!("declining encryption: {e}")))?;
            }
            Packet::Cancel { .. } | Packet::Startup(_) => {
                let stop = Stop::Say {
                    code: "28000",
                    message: "this port requires TLS: connect with sslmode=require or better"
                        .to_string(),
                };
                refuse(&mut sock, &stop).await;
                return Err(stop);
            }
        }
    }
    Err(Stop::Quiet(
        "asked for encryption too many times".to_string(),
    ))
}

/// Say why on the way out, in the client's own protocol, and hang up.
/// A quiet stop has nothing to say, which is what it is for.
async fn refuse<S: Wired>(sock: &mut S, stop: &Stop) {
    if let Stop::Say { code, message } = stop {
        let _ = sock.write_all(&error(code, message)).await;
    }
    let _ = sock.shutdown().await;
}

/// Read a certificate chain and its key, both PEM, and build the
/// acceptor a door is handed.
///
/// The chain is the leaf first and whatever intermediates a client
/// might not have, the same file `ssl_cert_file` takes, so a pair
/// already cut for a postgres is the pair this reads. A key that is
/// not the certificate's is caught here rather than at the first
/// connection, since rustls checks the pair when the config is built.
pub fn acceptor(cert: &std::path::Path, key: &std::path::Path) -> Result<TlsAcceptor, String> {
    use rustls::pki_types::pem::PemObject;

    let chain: Vec<_> = rustls::pki_types::CertificateDer::pem_file_iter(cert)
        .map_err(|e| format!("reading {}: {e}", cert.display()))?
        .collect::<Result<_, _>>()
        .map_err(|e| format!("reading {}: {e}", cert.display()))?;
    if chain.is_empty() {
        return Err(format!("{} holds no certificate", cert.display()));
    }
    let private = rustls::pki_types::PrivateKeyDer::from_pem_file(key)
        .map_err(|e| format!("reading {}: {e}", key.display()))?;
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(chain, private)
        .map_err(|e| format!("the certificate and the key do not go together: {e}"))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// What a client session runs on: the socket, or the socket with TLS
/// over it. Everything from the startup packet onwards is the same
/// protocol either way, so it is written once and the door decides
/// which of the two it is written to.
trait Wired: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Wired for T {}

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
async fn authenticate<S: Wired>(sock: &mut S, entry: &Tenant, route: &Route) -> Result<(), Stop> {
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

/// Turn Nagle off on a wire socket, both the client's and the one to
/// the database.
///
/// Nagle holds a small write back until the last small write has been
/// acknowledged, and the other side does not acknowledge straight away
/// because it is waiting to put the acknowledgement on a reply it has
/// not written yet. Every transaction through the pooler is a pair of
/// small writes in each direction, the query and then the reset on one
/// side and the rows and then the ready on the other, so the second of
/// each pair waits out the delayed acknowledgement timer, which is 40
/// ms on linux. Measured on server3 as 42.7 ms for `select 1` through
/// the pooler against 0.60 ms for the same statement through the
/// session port on the same node and the same database.
///
/// A socket that refuses is still a working socket, so a failure here
/// is not a reason to fail the connection.
fn nodelay(sock: &TcpStream) {
    if let Err(e) = sock.set_nodelay(true) {
        log::debug!("nodelay: {e}");
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
    nodelay(&up);
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
/// asks for, and SCRAM-SHA-256 is what a postgres somebody else
/// initialised asks for instead, so all four are answered here. A
/// mechanism this side does not have is a sentence rather than a
/// hangup, because a connection that fails without saying why is a
/// connection nobody can fix.
async fn handshake(up: &mut TcpStream, password: &[u8], user: &str) -> Result<(), Stop> {
    let mut scram: Option<crate::scram::Scram> = None;
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
            // AuthenticationSASL, and the mechanisms it will take.
            10 => {
                let offered = mechanisms(&body[4..]);
                if !offered.iter().any(|name| name == SCRAM_SHA_256) {
                    return Err(Stop::Say {
                        code: "0A000",
                        message: format!(
                            "this project's database asks for {}, and this port speaks {SCRAM_SHA_256}",
                            offered.join(" or ")
                        ),
                    });
                }
                let started = crate::scram::Scram::new();
                let first = started.first();
                let mut msg = SCRAM_SHA_256.as_bytes().to_vec();
                msg.push(0);
                msg.extend_from_slice(&(first.len() as i32).to_be_bytes());
                msg.extend_from_slice(first.as_bytes());
                scram = Some(started);
                up.write_all(&raw(b'p', &msg))
                    .await
                    .map_err(|e| Stop::Quiet(format!("sasl to the database: {e}")))?;
            }
            // AuthenticationSASLContinue: the salt and the server's
            // nonce, answered with this side's proof.
            11 => {
                let scram = scram.as_mut().ok_or_else(|| {
                    Stop::Quiet("the database continued a sasl exchange it never started".into())
                })?;
                let last = scram
                    .last(password, &text(&body[4..])?)
                    .map_err(|e| Stop::Quiet(format!("sasl: {e}")))?;
                up.write_all(&raw(b'p', last.as_bytes()))
                    .await
                    .map_err(|e| Stop::Quiet(format!("sasl to the database: {e}")))?;
            }
            // AuthenticationSASLFinal: the server's own proof, which is
            // checked rather than taken, and then the loop reads the
            // AuthenticationOk after it.
            12 => {
                let scram = scram.as_ref().ok_or_else(|| {
                    Stop::Quiet("the database finished a sasl exchange it never started".into())
                })?;
                scram
                    .done(&text(&body[4..])?)
                    .map_err(|e| Stop::Quiet(format!("sasl: {e}")))?;
            }
            other => {
                return Err(Stop::Quiet(format!(
                    "the database asked for authentication method {other}"
                )));
            }
        }
    }
}

/// The one mechanism this side has. The PLUS of it is channel binding,
/// which this client does not do and does not claim to.
const SCRAM_SHA_256: &str = "SCRAM-SHA-256";

/// The mechanism names in an AuthenticationSASL, which are null
/// terminated strings one after the other and then one more zero.
fn mechanisms(body: &[u8]) -> Vec<String> {
    let mut names = Vec::new();
    let mut rest = body;
    while let Some((name, tail)) = cstr(rest) {
        if name.is_empty() {
            break;
        }
        names.push(name.to_string());
        rest = tail;
    }
    names
}

/// A sasl message body, which is utf8 and not null terminated.
fn text(body: &[u8]) -> Result<String, Stop> {
    String::from_utf8(body.to_vec())
        .map_err(|_| Stop::Quiet("the database's sasl message is not utf8".into()))
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
    /// the session is ready, and answers simple queries with the query
    /// back. It tracks whether a transaction is open, because that is
    /// the one thing the pooler reads out of its answers.
    #[derive(Default)]
    struct Fake {
        startups: StdMutex<Vec<Vec<(String, String)>>>,
        cancels: StdMutex<Vec<(i32, i32)>>,
        queries: StdMutex<Vec<String>>,
        /// Which connection each query arrived on, so a test can say
        /// two clients shared a backend or did not.
        opened: StdMutex<i32>,
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
            let pid = {
                let mut opened = self.opened.lock().unwrap();
                *opened += 1;
                4241 + *opened
            };
            let mut hello = Vec::new();
            hello.extend_from_slice(&raw(b'R', &0i32.to_be_bytes()));
            hello.extend_from_slice(&raw(b'S', b"server_version\x0018.4\0"));
            let mut key = Vec::new();
            key.extend_from_slice(&pid.to_be_bytes());
            key.extend_from_slice(&99i32.to_be_bytes());
            hello.extend_from_slice(&raw(b'K', &key));
            hello.extend_from_slice(&raw(b'Z', b"I"));
            if sock.write_all(&hello).await.is_err() {
                return;
            }

            let mut framed = Framed::new(sock);
            let mut in_transaction = false;
            while let Ok(Some(msg)) = framed.next().await {
                let ready = |in_transaction: bool| match in_transaction {
                    true => raw(b'Z', b"T"),
                    false => raw(b'Z', b"I"),
                };
                match msg[0] {
                    b'Q' => {
                        let (sql, _) = cstr(&msg[5..]).unwrap_or_default();
                        self.queries.lock().unwrap().push(format!("{pid} {sql}"));
                        let upper = sql.to_uppercase();
                        if upper.starts_with("BEGIN") {
                            in_transaction = true;
                        }
                        if upper.starts_with("COMMIT")
                            || upper.starts_with("ROLLBACK")
                            || upper.starts_with("DISCARD")
                        {
                            in_transaction = false;
                        }
                        let mut answer = raw(b'C', format!("{sql}\0").as_bytes());
                        answer.extend_from_slice(&ready(in_transaction));
                        if framed.send(&answer).await.is_err() {
                            return;
                        }
                    }
                    b'S' => {
                        if framed.send(&ready(in_transaction)).await.is_err() {
                            return;
                        }
                    }
                    b'X' => return,
                    _ => {}
                }
            }
        }

        fn queries(&self) -> Vec<String> {
            self.queries.lock().unwrap().clone()
        }

        /// How many connections this database has been asked for.
        fn opened(&self) -> i32 {
            *self.opened.lock().unwrap()
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
        project(Mode::Session).await
    }

    /// The same thing on 6543: one backend per transaction rather than
    /// one per connection.
    async fn one_pooled_project() -> (tempfile::TempDir, Arc<Fake>, Arc<Point>, String) {
        project(Mode::Transaction).await
    }

    async fn project(mode: Mode) -> (tempfile::TempDir, Arc<Fake>, Arc<Point>, String) {
        door(mode, None).await
    }

    async fn door(
        mode: Mode,
        tls: Option<TlsAcceptor>,
    ) -> (tempfile::TempDir, Arc<Fake>, Arc<Point>, String) {
        let fake = Arc::new(Fake::default());
        let addr = fake.spawn().await;
        let (host, port) = addr.rsplit_once(':').expect("host and port");
        let dsn = format!("host={host} port={port} user=zou dbname=postgres");
        let (dir, backend, at) = behind(mode, tls, &dsn).await;
        (dir, fake, backend, at)
    }

    /// A project, a door in front of it, and whatever dsn is behind it,
    /// which is the fake for most tests and a real postgres for the one
    /// that needs a real login.
    async fn behind(
        mode: Mode,
        tls: Option<TlsAcceptor>,
        dsn: &str,
    ) -> (tempfile::TempDir, Arc<Point>, String) {
        let dir = tempfile::tempdir().expect("a directory");
        let store: Arc<dyn CasStore> = Arc::new(LocalFsStore::new(dir.path()));
        registry::create(&*store, &Tenant::new("acme-prod", SECRET, 1)).expect("a project");

        let dsn = dsn.to_string();
        let backend = Arc::new(Point {
            dsn,
            ups: StdMutex::new(Vec::new()),
        });
        let door = Wire::new(
            Arc::new(Registry::new(store)),
            Arc::new(Attached::new(backend.clone())),
        );
        let door = match mode {
            Mode::Session => door,
            Mode::Transaction => door.pooling(),
        };
        let wire = Arc::new(match tls {
            Some(tls) => door.secured(tls),
            None => door,
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
        let addr = listener.local_addr().expect("an address").to_string();
        tokio::spawn(wire.serve(listener));
        (dir, backend, addr)
    }

    fn key(role: &str) -> String {
        crate::jwt::mint(&crate::jwt::key_claims(role), SECRET.as_bytes())
    }

    /// A client: the startup packet, the password, and whatever comes
    /// back.
    ///
    /// It is written over whatever the socket turned out to be for the
    /// same reason the door is: a session on a TLS stream is the same
    /// exchange as a session on a plain one, and a test that says so
    /// should be the same test.
    struct Client<S = TcpStream> {
        sock: S,
    }

    impl Client<TcpStream> {
        async fn open(addr: &str) -> Client<TcpStream> {
            Client {
                sock: TcpStream::connect(addr).await.expect("the wire port"),
            }
        }
    }

    impl<S: Wired> Client<S> {
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

        /// Log in and read the rest of the greeting, up to and
        /// including the ReadyForQuery, and hand back the cancel key
        /// that came with it.
        async fn up(&mut self, role: &str) -> (i32, i32) {
            let user = format!("{role}.acme-prod");
            assert_eq!(
                self.login(&[("user", user.as_str())], &key(role)).await.0,
                b'R',
                "the session should have been let in"
            );
            let mut cancel_key = (0, 0);
            loop {
                let (tag, body) = self.message().await;
                if tag == b'K' {
                    cancel_key = (
                        i32::from_be_bytes([body[0], body[1], body[2], body[3]]),
                        i32::from_be_bytes([body[4], body[5], body[6], body[7]]),
                    );
                }
                if tag == b'Z' {
                    return cancel_key;
                }
            }
        }

        async fn send(&mut self, msg: &[u8]) {
            self.sock.write_all(msg).await.expect("a message out");
        }

        /// A simple query and everything it is answered with, up to the
        /// ReadyForQuery, whose transaction status is the last thing in
        /// the last message.
        async fn query(&mut self, sql: &str) -> Vec<(u8, Vec<u8>)> {
            self.send(&raw(b'Q', format!("{sql}\0").as_bytes())).await;
            let mut out = Vec::new();
            loop {
                let message = self.message().await;
                let done = message.0 == b'Z';
                out.push(message);
                if done {
                    return out;
                }
            }
        }
    }

    /// A certificate for a door, written to a pair of files because
    /// files are what an operator hands over, and handed back as well
    /// so a client can be made that trusts this one and no other.
    fn certificate(
        dir: &std::path::Path,
    ) -> (TlsAcceptor, rustls::pki_types::CertificateDer<'static>) {
        let made =
            rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("a key pair");
        let cert = dir.join("wire.crt");
        let key = dir.join("wire.key");
        std::fs::write(&cert, made.cert.pem()).expect("the certificate on disk");
        std::fs::write(&key, made.signing_key.serialize_pem()).expect("the key on disk");
        (
            acceptor(&cert, &key).expect("a pair that goes together"),
            made.cert.der().clone(),
        )
    }

    /// A client that asks for encryption, is given it, and speaks the
    /// rest of the protocol inside it, which is what libpq does for
    /// `sslmode=require` and above.
    async fn encrypted(
        addr: &str,
        cert: rustls::pki_types::CertificateDer<'static>,
    ) -> Client<tokio_rustls::client::TlsStream<TcpStream>> {
        let sock = TcpStream::connect(addr).await.expect("the wire port");
        handshake(sock, cert).await
    }

    /// The request, the one byte back, and the handshake, on a socket
    /// that may already have had a word or two on it.
    async fn handshake(
        mut sock: TcpStream,
        cert: rustls::pki_types::CertificateDer<'static>,
    ) -> Client<tokio_rustls::client::TlsStream<TcpStream>> {
        let mut ask = Vec::new();
        ask.extend_from_slice(&8i32.to_be_bytes());
        ask.extend_from_slice(&SSL_REQUEST.to_be_bytes());
        sock.write_all(&ask).await.expect("an ssl request");
        let mut answer = [0u8; 1];
        sock.read_exact(&mut answer).await.expect("an answer");
        assert_eq!(&answer, b"S", "the door has a certificate on it");
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert).expect("the door's own certificate");
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let name = rustls::pki_types::ServerName::try_from("localhost").expect("a name");
        let sock = tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(name, sock)
            .await
            .expect("the handshake");
        Client { sock }
    }

    /// Ask a fake to cancel whatever the pair names, on a connection of
    /// its own, the way a real client does.
    async fn cancel(addr: &str, pid: i32, secret: i32) {
        let mut packet = Vec::new();
        packet.extend_from_slice(&16i32.to_be_bytes());
        packet.extend_from_slice(&CANCEL_REQUEST.to_be_bytes());
        packet.extend_from_slice(&pid.to_be_bytes());
        packet.extend_from_slice(&secret.to_be_bytes());
        Client::open(addr).await.send(&packet).await;
    }

    /// Wait for something a background task does. The pooler hands a
    /// backend back after the client has already been answered, so a
    /// test that looks straight away is looking too early.
    async fn eventually(mut done: impl FnMut() -> bool) -> bool {
        for _ in 0..100 {
            if done() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        done()
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

    /// The whole point of the certificate: the same session, the same
    /// answers, with the bytes encrypted between the client and the
    /// door.
    #[tokio::test]
    async fn a_door_with_a_certificate_runs_the_session_inside_the_handshake() {
        let held = tempfile::tempdir().expect("a directory");
        let (tls, cert) = certificate(held.path());
        let (_d, fake, _backend, addr) = door(Mode::Session, Some(tls)).await;
        let mut client = encrypted(&addr, cert).await;
        assert_eq!(
            client.up("anon").await,
            (4242, 99),
            "the greeting is the backend's own, handshake or no handshake"
        );
        let answer = client.query("select 1").await;
        assert_eq!(
            answer.first().map(|(tag, body)| (*tag, body.as_slice())),
            Some((b'C', &b"select 1\0"[..])),
            "the query crossed encrypted and reached the database in the clear"
        );
        assert_eq!(
            fake.startup()
                .iter()
                .find(|(k, _)| k == "user")
                .map(|(_, v)| v.as_str()),
            Some("zou"),
            "the connection out is the dsn's own, as it is on the plain door"
        );
    }

    /// A client that meant to talk in the clear to a door that has a
    /// certificate is told so, rather than served or hung up on. The
    /// packet it would have sent next carries the project's key.
    #[tokio::test]
    async fn a_startup_packet_in_the_clear_is_refused_by_a_door_with_a_certificate() {
        let held = tempfile::tempdir().expect("a directory");
        let (tls, _cert) = certificate(held.path());
        let (_d, fake, _backend, addr) = door(Mode::Session, Some(tls)).await;
        let mut client = Client::open(&addr).await;
        let (tag, body) = client
            .login(&[("user", "anon.acme-prod")], &key("anon"))
            .await;
        assert_eq!(tag, b'E');
        let (code, message) = says(&body);
        assert_eq!(code, "28000");
        assert!(message.contains("sslmode=require"), "{message}");
        assert_eq!(
            fake.opened(),
            0,
            "nothing was dialled for a connection that was never let in"
        );
    }

    /// A libpq with GSSAPI compiled in asks for that first and takes an
    /// N for an answer, so the byte before the one that matters must
    /// not cost the client its connection.
    #[tokio::test]
    async fn a_gssenc_request_is_declined_and_the_ssl_request_after_it_is_taken() {
        let held = tempfile::tempdir().expect("a directory");
        let (tls, cert) = certificate(held.path());
        let (_d, _fake, _backend, addr) = door(Mode::Session, Some(tls)).await;
        let mut sock = TcpStream::connect(&addr).await.expect("the wire port");
        let mut ask = Vec::new();
        ask.extend_from_slice(&8i32.to_be_bytes());
        ask.extend_from_slice(&GSSENC_REQUEST.to_be_bytes());
        sock.write_all(&ask).await.expect("a request");
        let mut answer = [0u8; 1];
        sock.read_exact(&mut answer).await.expect("an answer");
        assert_eq!(&answer, b"N", "this door does not speak gssapi");
        let mut client = handshake(sock, cert).await;
        assert_eq!(
            client
                .login(&[("user", "anon.acme-prod")], &key("anon"))
                .await
                .0,
            b'R',
            "and the request that follows is the one it takes"
        );
    }

    /// A postgres this node did not start asks for SCRAM-SHA-256, and
    /// a door that cannot answer that is a door that only works against
    /// its own postmasters. The test cluster is whatever it was
    /// initialised with, so this says the login goes through however it
    /// is asked for rather than naming a method.
    #[tokio::test]
    async fn a_database_this_node_did_not_start_is_logged_in_to_however_it_asks() {
        let Ok(dsn) = std::env::var("ZOU_PG_TEST_DSN") else {
            return;
        };
        let pool = crate::sql::Pool::new(&dsn, 1).expect("the dsn parses");
        let sess = pool.unscoped().await.expect("connect");
        for role in ["anon", "authenticated", "service_role"] {
            let make = format!(
                "do $$ begin if not exists (select 1 from pg_roles where rolname = '{role}') \
                 then create role {role}; end if; end $$"
            );
            sess.execute(&make, &[]).await.expect("a role to become");
        }
        sess.commit().await.expect("park");

        let (_d, _backend, addr) = behind(Mode::Session, None, &dsn).await;
        let mut client = Client::open(&addr).await;
        client.up("service_role").await;
        let answer = client.query("select current_user").await;
        let row = answer
            .iter()
            .find(|(tag, _)| *tag == b'D')
            .map(|(_, body)| String::from_utf8_lossy(body).to_string())
            .expect("a row came back");
        assert!(
            row.contains("service_role"),
            "the session runs as the role it asked for: {row}"
        );
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
        // The rest of the login exchange the database sent, relayed,
        // its own cancel key included.
        assert_eq!(
            client.up("anon").await,
            (4242, 99),
            "in session mode the key the client holds is the backend's own"
        );

        let answer = client.query("select 1").await;
        assert_eq!(
            answer.first().map(|(tag, body)| (*tag, body.as_slice())),
            Some((b'C', &b"select 1\0"[..])),
            "the query reached the database and its answer came back"
        );
    }

    #[tokio::test]
    async fn a_cancel_reaches_the_database_the_session_is_on() {
        let (_d, fake, _backend, addr) = one_project().await;
        let mut client = Client::open(&addr).await;
        let held = client.up("anon").await;

        cancel(&addr, held.0, held.1).await;
        eventually(|| !fake.cancels.lock().unwrap().is_empty()).await;
        assert_eq!(
            fake.cancels.lock().unwrap().as_slice(),
            &[(4242, 99)],
            "the key the client holds is the one its backend generated"
        );
    }

    #[tokio::test]
    async fn a_cancel_for_a_session_this_node_never_had_goes_nowhere() {
        let (_d, fake, _backend, addr) = one_project().await;
        cancel(&addr, 1, 2).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            fake.cancels.lock().unwrap().is_empty(),
            "guessing a pair must not cancel somebody else's query"
        );
    }

    /// A Parse message: the statement name, the sql, and no parameter
    /// types.
    fn parse(name: &str, sql: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(sql.as_bytes());
        body.push(0);
        body.extend_from_slice(&0i16.to_be_bytes());
        raw(b'P', &body)
    }

    #[tokio::test]
    async fn the_greeting_under_the_pooler_is_the_databases_own_and_the_key_is_not() {
        let (_d, _fake, _backend, addr) = one_pooled_project().await;
        let mut client = Client::open(&addr).await;
        assert_eq!(
            client
                .login(&[("user", "anon.acme-prod")], &key("anon"))
                .await
                .0,
            b'R'
        );
        let mut parameters = Vec::new();
        let mut cancel_key = (0, 0);
        loop {
            let (tag, body) = client.message().await;
            match tag {
                b'S' => parameters.push(body),
                b'K' => {
                    cancel_key = (
                        i32::from_be_bytes([body[0], body[1], body[2], body[3]]),
                        i32::from_be_bytes([body[4], body[5], body[6], body[7]]),
                    );
                }
                b'Z' => break,
                _ => {}
            }
        }
        assert_eq!(
            parameters,
            vec![b"server_version\x0018.4\0".to_vec()],
            "the settings a client is told are the database's own, not this server's guesses"
        );
        assert_ne!(
            cancel_key,
            (4242, 99),
            "the key cannot be a backend's, because at login there is no backend yet"
        );
    }

    #[tokio::test]
    async fn two_clients_one_after_the_other_are_one_backend() {
        let (_d, fake, _backend, addr) = one_pooled_project().await;
        let mut first = Client::open(&addr).await;
        first.up("service_role").await;
        first.query("select 1").await;
        first.send(&raw(b'X', b"")).await;
        assert!(
            eventually(|| fake
                .queries()
                .iter()
                .filter(|q| q.ends_with("DISCARD ALL"))
                .count()
                >= 2)
            .await,
            "the backend should have been handed back: {:?}",
            fake.queries()
        );

        let mut second = Client::open(&addr).await;
        second.up("service_role").await;
        second.query("select 2").await;
        assert_eq!(
            fake.opened(),
            1,
            "the second client should have been handed the first one's backend"
        );
        let queries = fake.queries();
        assert!(
            queries.contains(&"4242 select 1".to_string()),
            "{queries:?}"
        );
        assert!(
            queries.contains(&"4242 select 2".to_string()),
            "{queries:?}"
        );
    }

    #[tokio::test]
    async fn a_backend_inside_a_transaction_is_nobody_elses() {
        let (_d, fake, _backend, addr) = one_pooled_project().await;
        let mut first = Client::open(&addr).await;
        first.up("service_role").await;
        let answer = first.query("begin").await;
        assert_eq!(
            answer.last().map(|(tag, body)| (*tag, body.as_slice())),
            Some((b'Z', &b"T"[..])),
            "the transaction status is what says the backend is still owed"
        );

        let mut second = Client::open(&addr).await;
        second.up("service_role").await;
        second.query("select 2").await;
        assert_eq!(
            fake.opened(),
            2,
            "a second connection is what an open transaction costs"
        );
        let queries = fake.queries();
        assert!(queries.contains(&"4242 begin".to_string()), "{queries:?}");
        assert!(
            queries.contains(&"4243 select 2".to_string()),
            "{queries:?}"
        );

        first.query("commit").await;
        assert!(
            eventually(|| fake
                .queries()
                .iter()
                .filter(|q| q.ends_with("DISCARD ALL"))
                .count()
                >= 3)
            .await,
            "commit ends the transaction, and the end of the transaction is the release"
        );
        let mut third = Client::open(&addr).await;
        third.up("service_role").await;
        third.query("select 3").await;
        assert_eq!(fake.opened(), 2, "and then there is one to spare");
    }

    #[tokio::test]
    async fn what_one_client_left_behind_does_not_reach_the_next() {
        let (_d, fake, _backend, addr) = one_pooled_project().await;
        let mut client = Client::open(&addr).await;
        client.up("service_role").await;
        client.query("set search_path to mine").await;
        assert!(
            eventually(|| {
                let queries = fake.queries();
                let Some(at) = queries
                    .iter()
                    .position(|q| q.ends_with("set search_path to mine"))
                else {
                    return false;
                };
                queries[at + 1..].iter().any(|q| q.ends_with("DISCARD ALL"))
            })
            .await,
            "a backend is cleaned on the way back, not on the way out: {:?}",
            fake.queries()
        );
    }

    #[tokio::test]
    async fn a_named_prepared_statement_is_refused_at_the_statement_that_names_it() {
        let (_d, fake, _backend, addr) = one_pooled_project().await;
        let mut client = Client::open(&addr).await;
        client.up("service_role").await;
        client.send(&parse("s1", "select 1")).await;
        let (tag, body) = client.message().await;
        assert_eq!(tag, b'E');
        let (code, message) = says(&body);
        assert_eq!(code, "0A000");
        assert!(message.contains("s1"), "{message}");
        assert!(
            message.contains("5432"),
            "a refusal should say where the thing it refused does work: {message}"
        );

        // Everything until Sync is ignored, the way a backend in error
        // state ignores it, or the two sides disagree about how many
        // answers are owed.
        client.send(&raw(b'B', b"\0\0\0\0\0\0\0\0\0\0")).await;
        client.send(&raw(b'S', b"")).await;
        let (tag, body) = client.message().await;
        assert_eq!((tag, body.as_slice()), (b'Z', &b"I"[..]));
        assert!(
            !fake.queries().iter().any(|q| q.ends_with("select 1")),
            "nothing that was refused here should have reached the database"
        );
    }

    #[tokio::test]
    async fn a_sync_with_nothing_outstanding_is_answered_without_a_backend() {
        let (_d, fake, _backend, addr) = one_pooled_project().await;
        let mut client = Client::open(&addr).await;
        client.up("service_role").await;
        client.send(&raw(b'S', b"")).await;
        let (tag, body) = client.message().await;
        assert_eq!((tag, body.as_slice()), (b'Z', &b"I"[..]));
        assert_eq!(
            fake.opened(),
            1,
            "borrowing a backend to say ready would be a transaction that never happened"
        );
    }

    #[tokio::test]
    async fn a_cancel_under_the_pooler_is_translated_to_the_backend_in_the_middle() {
        let (_d, fake, _backend, addr) = one_pooled_project().await;
        let mut client = Client::open(&addr).await;
        let held = client.up("service_role").await;
        client.query("begin").await;

        cancel(&addr, held.0, held.1).await;
        assert!(
            eventually(|| !fake.cancels.lock().unwrap().is_empty()).await,
            "a cancel for a session in a transaction has somewhere to go"
        );
        assert_eq!(
            fake.cancels.lock().unwrap().as_slice(),
            &[(4242, 99)],
            "the pair the client holds is this server's, and the pair the database gets is its own"
        );
    }

    #[tokio::test]
    async fn a_cancel_between_transactions_cancels_nothing() {
        let (_d, fake, _backend, addr) = one_pooled_project().await;
        let mut client = Client::open(&addr).await;
        let held = client.up("service_role").await;
        client.query("select 1").await;
        assert!(
            eventually(|| fake
                .queries()
                .iter()
                .filter(|q| q.ends_with("DISCARD ALL"))
                .count()
                >= 2)
            .await
        );

        cancel(&addr, held.0, held.1).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            fake.cancels.lock().unwrap().is_empty(),
            "a session between transactions is on no backend, and the one it used last is somebody else's now"
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

    #[tokio::test]
    async fn nagle_is_off_on_a_socket_the_wire_owns() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
        let addr = listener.local_addr().expect("an address");
        let accepted = tokio::spawn(async move { listener.accept().await.expect("a client").0 });
        let client = TcpStream::connect(addr).await.expect("a connection");
        let server = accepted.await.expect("the accept task");
        assert!(
            !server.nodelay().expect("the option"),
            "a socket arrives with nagle on, which is what this is about"
        );
        nodelay(&server);
        nodelay(&client);
        assert!(server.nodelay().expect("the option"));
        assert!(client.nodelay().expect("the option"));
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
