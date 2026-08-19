//! `Deno.connect`, which is a function opening a socket to somebody
//! else and speaking whatever protocol it likes over it.
//!
//! The thing that asks is a database driver. `postgres-on-the-edge` in
//! the Supabase examples imports `deno-postgres`, and that driver is a
//! `Deno.connect`, a `Deno.startTls` when the server offers one, and a
//! read and a write loop over the wire protocol. There is no way to
//! reach a database from a function without a socket, so this is the
//! last named api in that corpus that was missing rather than a shape
//! that was wrong.
//!
//! What a function may reach is not restricted, which is the same
//! answer `fetch` gives and for the same reason: a function is the
//! project's own code, and a runtime that let it call any http port on
//! any host and stopped it opening a tcp one to the same host would be
//! drawing a line that means nothing. Two things are drawn instead, and
//! both are about the host rather than the network. A unix socket is a
//! file on the machine the function is running on, so `transport: unix`
//! is refused by name rather than handed the host's own file system.
//! And an isolate may hold [`OPEN`] sockets at once, so a function that
//! leaks one per call fails loudly on the box it is running on instead
//! of quietly taking every descriptor on it.
//!
//! The connection is split in two, the same as a websocket is, because
//! a protocol where both ends talk at once is the ordinary case and one
//! lock over the whole socket would make a write wait for a read that
//! may never come.

use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;

use deno_core::{OpState, ToJsBuffer, op2};
use rustls::pki_types::ServerName;
use rustls::pki_types::pem::PemObject;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

/// How long a connection, or the handshake on top of one, may take.
/// The same reasoning as `fetch`'s timeout: a host that accepts and
/// then says nothing is otherwise an isolate that never comes back.
const CONNECT: Duration = Duration::from_secs(30);

/// The most one read op will ask the network for, however large the
/// buffer the caller handed in is. A read that answers with less than
/// was asked for is what a read is, and a function looping until it has
/// what it wants is written that way already.
const CHUNK: usize = 64 * 1024;

/// How many sockets one isolate may hold open at once.
pub const OPEN: usize = 256;

/// A socket, before and after it has TLS on it.
///
/// An enum rather than a boxed trait object, because the two arms are
/// known here and a trait object of a subtrait is not the supertrait.
enum Held {
    Plain(TcpStream),
    Secure(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for Held {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Held::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Held::Secure(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Held {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Held::Plain(stream) => Pin::new(stream).poll_write(cx, bytes),
            Held::Secure(stream) => Pin::new(stream).poll_write(cx, bytes),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Held::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Held::Secure(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Held::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Held::Secure(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

/// One open connection, in two halves that are locked separately.
///
/// The addresses are not kept beside them: javascript is told both of
/// them when the socket is opened and holds them itself, which is what
/// `conn.localAddr` is there.
struct Socket {
    reader: Rc<Mutex<ReadHalf<Held>>>,
    writer: Rc<Mutex<WriteHalf<Held>>>,
}

/// Every socket this isolate has open, by the id javascript holds.
#[derive(Default)]
pub struct Streams {
    last: u32,
    open: HashMap<u32, Socket>,
}

/// One end of a connection, in the shape `conn.localAddr` has.
#[derive(serde::Serialize)]
pub struct Where {
    transport: &'static str,
    hostname: String,
    port: u16,
}

impl Where {
    fn of(addr: io::Result<std::net::SocketAddr>) -> Where {
        match addr {
            Ok(addr) => Where {
                transport: "tcp",
                hostname: addr.ip().to_string(),
                port: addr.port(),
            },
            // A socket whose address the kernel will not say is still a
            // socket, so this is a shape rather than a failure.
            Err(_) => Where {
                transport: "tcp",
                hostname: String::new(),
                port: 0,
            },
        }
    }
}

/// What a connection attempt did, in the one shape javascript reads:
/// `kind` says which of the fields under it means anything, and `name`
/// is the `Deno.errors` class a failure is thrown as.
#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Made {
    Opened {
        rid: u32,
        local: Where,
        remote: Where,
    },
    Failed {
        name: &'static str,
        why: String,
    },
}

/// What a read got, where the end of the stream is its own answer
/// rather than zero bytes, because `conn.read` answers `null` for it.
#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Got {
    Bytes { bytes: ToJsBuffer },
    Eof,
    Failed { name: &'static str, why: String },
}

/// What a write did, which is a count and not a promise that all of it
/// went: a short write is a write.
#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Wrote {
    Sent { sent: u32 },
    Failed { name: &'static str, why: String },
}

/// A connection, and the id the function holds it by.
#[op2(async(lazy), fast)]
#[serde]
pub async fn op_zou_tcp_connect(
    state: Rc<RefCell<OpState>>,
    #[string] hostname: String,
    #[smi] port: u32,
) -> Made {
    if let Some(full) = full(&state) {
        return full;
    }
    let port = port as u16;
    match dialled(&hostname, port).await {
        Err(failed) => failed,
        Ok(stream) => kept(&state, Held::Plain(stream)),
    }
}

/// The same, with the handshake done before the function is given
/// anything to hold.
#[op2(async(lazy))]
#[serde]
pub async fn op_zou_tcp_connect_tls(
    state: Rc<RefCell<OpState>>,
    #[string] hostname: String,
    #[smi] port: u32,
    #[serde] authorities: Vec<String>,
) -> Made {
    if let Some(full) = full(&state) {
        return full;
    }
    let port = port as u16;
    let plain = match dialled(&hostname, port).await {
        Err(failed) => return failed,
        Ok(stream) => stream,
    };
    match secured(plain, &hostname, &authorities).await {
        Err(failed) => failed,
        Ok(stream) => kept(&state, stream),
    }
}

/// TLS on top of a socket the function already has, which is what a
/// protocol that asks the server whether it speaks TLS before it
/// speaks it needs: postgres is one, and so is every STARTTLS there
/// has ever been.
///
/// The socket that went in is gone afterwards and what comes back is a
/// new one, the same as upstream, where the connection is moved into
/// the TLS stream rather than wrapped in place.
#[op2(async(lazy))]
#[serde]
pub async fn op_zou_tcp_start_tls(
    state: Rc<RefCell<OpState>>,
    #[smi] rid: u32,
    #[string] hostname: String,
    #[serde] authorities: Vec<String>,
) -> Made {
    let taken = {
        let mut state = state.borrow_mut();
        state.borrow_mut::<Streams>().open.remove(&rid)
    };
    let Some(socket) = taken else {
        return Made::Failed {
            name: "BadResource",
            why: "that connection is closed".to_string(),
        };
    };
    let (Ok(reader), Ok(writer)) = (
        Rc::try_unwrap(socket.reader).map(Mutex::into_inner),
        Rc::try_unwrap(socket.writer).map(Mutex::into_inner),
    ) else {
        // A read or a write is still out there holding half of this,
        // and the two halves have to be put back together before there
        // is a stream to hand to the handshake at all.
        return Made::Failed {
            name: "BadResource",
            why: "a connection cannot be given TLS while a read or a write of it is in flight"
                .to_string(),
        };
    };
    let Held::Plain(stream) = reader.unsplit(writer) else {
        return Made::Failed {
            name: "InvalidData",
            why: "that connection already has TLS on it".to_string(),
        };
    };
    match secured(stream, &hostname, &authorities).await {
        Err(failed) => failed,
        Ok(stream) => kept(&state, stream),
    }
}

/// The next thing the other end said, or the end of the stream.
#[op2(async(lazy), fast)]
#[serde]
pub async fn op_zou_tcp_read(
    state: Rc<RefCell<OpState>>,
    #[smi] rid: u32,
    #[smi] want: u32,
) -> Got {
    let Some(reader) = reader(&state, rid) else {
        return Got::Failed {
            name: "BadResource",
            why: "that connection is closed".to_string(),
        };
    };
    let want = (want as usize).min(CHUNK);
    let mut bytes = vec![0u8; want];
    let mut reader = reader.lock().await;
    match tokio::io::AsyncReadExt::read(&mut *reader, &mut bytes).await {
        Ok(0) => Got::Eof,
        Ok(read) => {
            bytes.truncate(read);
            Got::Bytes {
                bytes: bytes.into(),
            }
        }
        Err(e) => Got::Failed {
            name: named(&e),
            why: e.to_string(),
        },
    }
}

/// Bytes out, and how many of them went.
#[op2(async(lazy), fast)]
#[serde]
pub async fn op_zou_tcp_write(
    state: Rc<RefCell<OpState>>,
    #[smi] rid: u32,
    #[buffer(copy)] bytes: Vec<u8>,
) -> Wrote {
    let Some(writer) = writer(&state, rid) else {
        return Wrote::Failed {
            name: "BadResource",
            why: "that connection is closed".to_string(),
        };
    };
    let mut writer = writer.lock().await;
    // Flushed rather than left in the TLS stream's own buffer, because
    // a driver that wrote a message and is now waiting to be answered
    // has said everything it is going to say.
    let sent = match writer.write(&bytes).await {
        Ok(sent) => sent,
        Err(e) => {
            return Wrote::Failed {
                name: named(&e),
                why: e.to_string(),
            };
        }
    };
    match writer.flush().await {
        Ok(()) => Wrote::Sent { sent: sent as u32 },
        Err(e) => Wrote::Failed {
            name: named(&e),
            why: e.to_string(),
        },
    }
}

/// This end has nothing more to send, which the other end is told about
/// and which leaves the reading half open.
#[op2(async(lazy), fast)]
pub async fn op_zou_tcp_shutdown(state: Rc<RefCell<OpState>>, #[smi] rid: u32) {
    let Some(writer) = writer(&state, rid) else {
        return;
    };
    let mut writer = writer.lock().await;
    let _ = writer.shutdown().await;
}

/// Let go of a socket, which closes it: nothing else holds one.
#[op2(fast)]
pub fn op_zou_tcp_close(state: &mut OpState, #[smi] rid: u32) {
    state.borrow_mut::<Streams>().open.remove(&rid);
}

/// Whether this isolate is already holding as many sockets as it may,
/// as the answer to give if it is.
fn full(state: &Rc<RefCell<OpState>>) -> Option<Made> {
    let mut state = state.borrow_mut();
    let streams = state.borrow_mut::<Streams>();
    (streams.open.len() >= OPEN).then(|| Made::Failed {
        name: "Error",
        why: format!(
            "a function may hold {OPEN} sockets open at once, and this one already holds that many"
        ),
    })
}

/// The connection itself, with the two ways it does not happen said in
/// the words `Deno.errors` has for them.
async fn dialled(hostname: &str, port: u16) -> Result<TcpStream, Made> {
    let connecting = TcpStream::connect((hostname, port));
    let answered = tokio::time::timeout(CONNECT, connecting)
        .await
        .map_err(|_| Made::Failed {
            name: "TimedOut",
            why: format!(
                "no answer from {hostname}:{port} within {} seconds",
                CONNECT.as_secs()
            ),
        })?;
    let stream = answered.map_err(|e| Made::Failed {
        name: named(&e),
        why: format!("connecting to {hostname}:{port}: {e}"),
    })?;
    // The same default upstream has. A driver that writes a small
    // message and waits to be answered is the shape this matters most
    // for, and postgres is exactly that.
    let _ = stream.set_nodelay(true);
    Ok(stream)
}

/// The handshake, against the Mozilla roots plus whatever the function
/// handed in.
async fn secured(stream: TcpStream, hostname: &str, authorities: &[String]) -> Result<Held, Made> {
    let config = trusting(authorities).map_err(|why| Made::Failed {
        name: "InvalidData",
        why,
    })?;
    let name = ServerName::try_from(hostname.to_string()).map_err(|_| Made::Failed {
        name: "InvalidData",
        why: format!("{hostname} is not a name a certificate can be checked against"),
    })?;
    let connector = tokio_rustls::TlsConnector::from(config);
    let shaken = tokio::time::timeout(CONNECT, connector.connect(name, stream))
        .await
        .map_err(|_| Made::Failed {
            name: "TimedOut",
            why: format!(
                "the TLS handshake with {hostname} did not finish within {} seconds",
                CONNECT.as_secs()
            ),
        })?;
    let stream = shaken.map_err(|e| Made::Failed {
        name: named(&e),
        why: format!("the TLS handshake with {hostname} failed: {e}"),
    })?;
    Ok(Held::Secure(Box::new(stream)))
}

/// What a certificate is checked against.
///
/// The Mozilla bundle is built once, because it is a hundred and fifty
/// certificates and a driver that reconnects should not pay for it
/// every time. A function that named its own authorities gets its own
/// config, which is the uncommon case and is allowed to cost more.
fn trusting(authorities: &[String]) -> Result<Arc<rustls::ClientConfig>, String> {
    if authorities.is_empty() {
        static USUAL: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
        if let Some(usual) = USUAL.get() {
            return Ok(Arc::clone(usual));
        }
        let usual = Arc::new(checking(rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        })?);
        return Ok(Arc::clone(USUAL.get_or_init(|| usual)));
    }
    let mut roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    for pem in authorities {
        let mut added = 0;
        for certificate in rustls::pki_types::CertificateDer::pem_slice_iter(pem.as_bytes()) {
            let certificate =
                certificate.map_err(|e| format!("a certificate in caCerts did not parse: {e}"))?;
            roots
                .add(certificate)
                .map_err(|e| format!("a certificate in caCerts was not usable: {e}"))?;
            added += 1;
        }
        if added == 0 {
            return Err("a certificate in caCerts has no PEM certificate in it".to_string());
        }
    }
    Ok(Arc::new(checking(roots)?))
}

/// The client side of TLS, with the provider named rather than taken
/// from process state, so nothing else in the host application can
/// change what a function negotiates by installing a default first.
fn checking(roots: rustls::RootCertStore) -> Result<rustls::ClientConfig, String> {
    Ok(
        rustls::ClientConfig::builder_with_provider(
            rustls::crypto::ring::default_provider().into(),
        )
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("tls setup: {e}"))?
        .with_root_certificates(roots)
        .with_no_client_auth(),
    )
}

/// Hold a connected stream and answer with the id it is held by.
fn kept(state: &Rc<RefCell<OpState>>, stream: Held) -> Made {
    let (local, remote) = match &stream {
        Held::Plain(stream) => (
            Where::of(stream.local_addr()),
            Where::of(stream.peer_addr()),
        ),
        Held::Secure(stream) => (
            Where::of(stream.get_ref().0.local_addr()),
            Where::of(stream.get_ref().0.peer_addr()),
        ),
    };
    let (reader, writer) = tokio::io::split(stream);
    let mut state = state.borrow_mut();
    let streams = state.borrow_mut::<Streams>();
    streams.last += 1;
    let rid = streams.last;
    streams.open.insert(
        rid,
        Socket {
            reader: Rc::new(Mutex::new(reader)),
            writer: Rc::new(Mutex::new(writer)),
        },
    );
    Made::Opened { rid, local, remote }
}

fn reader(state: &Rc<RefCell<OpState>>, rid: u32) -> Option<Rc<Mutex<ReadHalf<Held>>>> {
    let mut state = state.borrow_mut();
    let streams = state.borrow_mut::<Streams>();
    streams
        .open
        .get(&rid)
        .map(|socket| Rc::clone(&socket.reader))
}

fn writer(state: &Rc<RefCell<OpState>>, rid: u32) -> Option<Rc<Mutex<WriteHalf<Held>>>> {
    let mut state = state.borrow_mut();
    let streams = state.borrow_mut::<Streams>();
    streams
        .open
        .get(&rid)
        .map(|socket| Rc::clone(&socket.writer))
}

/// The `Deno.errors` class a failure is thrown as, so a library that
/// branches on one takes the same branch here as it does there.
/// Anything else is a plain `Error`, which is what a name javascript
/// has never heard of would be anyway.
fn named(e: &io::Error) -> &'static str {
    match e.kind() {
        io::ErrorKind::ConnectionRefused => "ConnectionRefused",
        io::ErrorKind::ConnectionReset => "ConnectionReset",
        io::ErrorKind::ConnectionAborted => "ConnectionAborted",
        io::ErrorKind::NotConnected => "NotConnected",
        io::ErrorKind::BrokenPipe => "BrokenPipe",
        io::ErrorKind::TimedOut => "TimedOut",
        io::ErrorKind::Interrupted => "Interrupted",
        io::ErrorKind::InvalidData => "InvalidData",
        io::ErrorKind::UnexpectedEof => "UnexpectedEof",
        io::ErrorKind::NotFound => "NotFound",
        io::ErrorKind::PermissionDenied => "PermissionDenied",
        _ => "Error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The names a driver catches, which are the point of the mapping
    /// existing at all: a retry loop looks for a refusal by name.
    #[test]
    fn what_went_wrong_is_said_in_the_words_deno_has_for_it() {
        assert_eq!(
            named(&io::Error::from(io::ErrorKind::ConnectionRefused)),
            "ConnectionRefused"
        );
        assert_eq!(
            named(&io::Error::from(io::ErrorKind::BrokenPipe)),
            "BrokenPipe"
        );
        assert_eq!(named(&io::Error::from(io::ErrorKind::TimedOut)), "TimedOut");
        // Not every kind has a class, and one that has none is an
        // Error rather than a name nothing defines.
        assert_eq!(named(&io::Error::from(io::ErrorKind::WouldBlock)), "Error");
    }

    #[test]
    fn the_usual_roots_are_built_once_and_handed_out_again() {
        let one = trusting(&[]).expect("a config");
        let two = trusting(&[]).expect("a config");
        assert!(Arc::ptr_eq(&one, &two));
    }

    /// Something that is not a certificate is refused where it was
    /// handed in, rather than at a handshake that would say something
    /// about the server instead.
    #[test]
    fn a_ca_that_is_not_a_certificate_is_refused_by_saying_so() {
        let refused = trusting(&["not a certificate".to_string()]).expect_err("no certificate");
        assert!(refused.contains("caCerts"), "{refused}");
        let refused = trusting(&[
            "-----BEGIN CERTIFICATE-----\nbm90IGEgY2VydGlmaWNhdGU=\n-----END CERTIFICATE-----\n"
                .to_string(),
        ])
        .expect_err("not a certificate");
        assert!(refused.contains("caCerts"), "{refused}");
    }
}
