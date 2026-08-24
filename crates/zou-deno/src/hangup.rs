//! The socket under a `fetch`, so that an abort reaches the other end.
//!
//! A call out is made on a blocking thread by a blocking client, and the
//! op that awaits it can stop awaiting at any moment: that is what a
//! signal does. Stopping the awaiting is not stopping the call, though,
//! and the difference is the whole of this module. Without it the
//! request goes out, the server builds an answer, and the answer is
//! written to a reader that has gone; the server is never told, and the
//! thread is held for as long as the call would have taken.
//!
//! What is needed is a handle on the socket that somebody other than the
//! thread reading it can close, and the client does not offer one: its
//! own TCP transport keeps the stream to itself. So this is a transport
//! of its own, sitting where the client's would, under TLS rather than
//! over it, because the thing worth closing is the socket and not the
//! session on top of it.
//!
//! # A call is a ticket, and a ticket is what a socket is filed under
//!
//! Sockets are opened by a connector deep inside the client, which is
//! handed a uri and a config and nothing about whose call it is. The
//! call says whose it is another way: it puts its ticket on the thread
//! for as long as the client is running on it, and the transport reads
//! it from there the first time it is used for that ticket and files a
//! second handle on the socket under it.
//!
//! Filing it on use rather than on connect is what makes a kept
//! connection work. The client pools connections, so the second call to
//! a host is made on the first call's socket and no connector runs at
//! all; a socket filed at connect time would be filed under a call that
//! has been over for a minute.
//!
//! # What it does not cover
//!
//! A call through a CONNECT proxy is tunnelled by the client's own
//! connector, which opens that socket itself, so an abort of a proxied
//! call is the waiting again. Nothing here configures a proxy: it would
//! have to come from the environment of the process the function is
//! running in.

use std::cell::Cell;
use std::collections::HashMap;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use ureq::Error;
use ureq::config::Config;
use ureq::unversioned::resolver::DefaultResolver;
use ureq::unversioned::transport::{
    Buffers, ConnectProxyConnector, ConnectionDetails, Connector, Either, LazyBuffers, NextTimeout,
    RustlsConnector, Transport,
};

/// The ticket a thread carries when nobody's call is on it, which is
/// every thread that is not running one: the module loader's fetches
/// are made on this and are nobody's to abort.
const NOBODY: u64 = 0;

thread_local! {
    /// Whose call this thread is running, read by the transport.
    static MINE: Cell<u64> = const { Cell::new(NOBODY) };
}

/// Every socket a call in flight is holding, by the call.
///
/// A call has more than one when it was redirected, and each of them is
/// worth closing: the one being read now is the only one still open,
/// and closing an already closed socket is an error nobody looks at.
fn held() -> &'static Mutex<HashMap<u64, Vec<Arc<TcpStream>>>> {
    static HELD: OnceLock<Mutex<HashMap<u64, Vec<Arc<TcpStream>>>>> = OnceLock::new();
    HELD.get_or_init(Mutex::default)
}

/// One call out, named so that it can be ended from another thread.
pub(crate) struct Ticket(u64);

impl Ticket {
    /// A name no other call in this process has.
    ///
    /// Process wide rather than per isolate, because the sockets are in
    /// one place and two isolates number their calls the same way.
    pub(crate) fn new() -> Ticket {
        static NEXT: AtomicU64 = AtomicU64::new(NOBODY + 1);
        Ticket(NEXT.fetch_add(1, Ordering::Relaxed))
    }

    /// What to say to end this call, which is all the ending needs.
    pub(crate) fn id(&self) -> u64 {
        self.0
    }

    /// Run the call with this ticket on the thread, and leave nothing
    /// behind whether it answered, failed or panicked.
    pub(crate) fn during<T>(self, call: impl FnOnce() -> T) -> T {
        struct Until(u64);
        impl Drop for Until {
            fn drop(&mut self) {
                MINE.with(|mine| mine.set(NOBODY));
                if let Ok(mut held) = held().lock() {
                    held.remove(&self.0);
                }
            }
        }
        MINE.with(|mine| mine.set(self.0));
        let _until = Until(self.0);
        call()
    }
}

/// End a call's connections, from whatever thread noticed.
///
/// Shutting a socket down under the thread reading it is what makes the
/// read return rather than wait: the thread ends with an error nobody
/// hands anywhere, and the server on the other end reads the end of the
/// stream rather than writing into one.
pub(crate) fn hangup(ticket: u64) {
    let Ok(mut held) = held().lock() else { return };
    for socket in held.remove(&ticket).unwrap_or_default() {
        let _ = socket.shutdown(Shutdown::Both);
    }
}

/// The client, with this module's transport where its own would be.
pub(crate) fn agent(config: Config) -> ureq::Agent {
    // The order the client's own default has, with `Watched` standing
    // where its TCP connector stands: a proxy tunnel first, since that
    // opens its own socket and this one then has nothing to open, and
    // TLS last, since TLS wraps whatever the socket turned out to be.
    let connector =
        ().chain(ConnectProxyConnector::default())
            .chain(Watched)
            .chain(RustlsConnector::default());
    ureq::Agent::with_parts(config, connector, DefaultResolver::default())
}

/// Opens the socket the client would otherwise open for itself.
#[derive(Debug, Default)]
struct Watched;

impl<In: Transport> Connector<In> for Watched {
    type Out = Either<In, Socket>;

    fn connect(
        &self,
        details: &ConnectionDetails,
        chained: Option<In>,
    ) -> Result<Option<Self::Out>, Error> {
        // Somebody earlier in the chain already has a connection, which
        // is the proxy tunnel, and it is theirs rather than this one's.
        if let Some(tunnelled) = chained {
            return Ok(Some(Either::A(tunnelled)));
        }
        let config = details.config;
        let stream = dial(details)?;
        let buffers = LazyBuffers::new(config.input_buffer_size(), config.output_buffer_size());
        Ok(Some(Either::B(Socket::new(stream, buffers))))
    }
}

/// The first address that answers.
///
/// An address that could not be opened is a reason to try the next one,
/// whatever the reason was, and however long it took to say so. This
/// used to move on only after a refusal, on the grounds that anything
/// else would say the same thing again, which is true of the address
/// and not of the host: a name with an AAAA and an A record, on a box
/// with a v6 default route and nothing behind it, does not answer for
/// the first address and serves the whole graph off the second.
///
/// So the budget is split between them, which is what the client's own
/// connector does and what this stopped doing when it took the socket
/// over. Handing the first address the whole of it is the same bug in
/// slower clothes: a route that blackholes rather than refuses spends
/// every second the call had and the address that works is never
/// reached. Both shapes were on the box the examples corpus was
/// measured on, and both were every function answering 500 while curl
/// to the same url was a 200. See #632.
fn dial(details: &ConnectionDetails) -> Result<TcpStream, Error> {
    let between = details.addrs.len().max(1) as u32;
    let each = details.timeout.not_zero().map(|when| *when / between);
    let nodelay = details.config.no_delay();
    first(&details.addrs, |addr| one(addr, each, nodelay)).map_err(|e| match timed_out(&e) {
        true => Error::Timeout(details.timeout.reason),
        false => Error::Io(e),
    })
}

/// The walk itself, over whatever opening one address means.
///
/// What comes back when none of them worked is what the last one said,
/// since a caller reading it is trying to find out why nothing worked
/// and an invented refusal is a worse answer than a real unreachable.
fn first<T>(
    addrs: &[SocketAddr],
    mut open: impl FnMut(SocketAddr) -> io::Result<T>,
) -> io::Result<T> {
    let mut said = None;
    for addr in addrs {
        match open(*addr) {
            Ok(opened) => return Ok(opened),
            Err(e) => said = Some(e),
        }
    }
    Err(said
        .unwrap_or_else(|| io::Error::new(io::ErrorKind::ConnectionRefused, "Connection refused")))
}

fn one(addr: SocketAddr, within: Option<Duration>, nodelay: bool) -> io::Result<TcpStream> {
    let stream = match within {
        Some(when) => TcpStream::connect_timeout(&addr, when),
        None => TcpStream::connect(addr),
    }?;
    if nodelay {
        stream.set_nodelay(true)?;
    }
    Ok(stream)
}

/// A read or a write that ran out of time.
///
/// Two kinds because that is what the platforms answer: a socket with a
/// timeout on it reports `TimedOut` on unix and `WouldBlock` on windows.
fn timed_out(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    )
}

/// One TCP connection, as the client sees it and as an abort sees it.
struct Socket {
    /// Shared because the whole point is that a second handle on it can
    /// be filed somewhere another thread can reach.
    stream: Arc<TcpStream>,
    buffers: LazyBuffers,
    /// The last timeout asked of the socket, so that a call that asks
    /// for the same one twice is not two more system calls.
    writing: Option<Duration>,
    reading: Option<Duration>,
    /// The call this socket is currently filed under, which changes
    /// when a kept connection is handed to the next call.
    caller: AtomicU64,
}

impl Socket {
    fn new(stream: TcpStream, buffers: LazyBuffers) -> Socket {
        Socket {
            stream: Arc::new(stream),
            buffers,
            writing: None,
            reading: None,
            caller: AtomicU64::new(NOBODY),
        }
    }

    /// File this socket under whoever is calling, once per call.
    fn filed(&self) {
        let now = MINE.with(Cell::get);
        if now == self.caller.load(Ordering::Relaxed) {
            return;
        }
        self.caller.store(now, Ordering::Relaxed);
        if now == NOBODY {
            return;
        }
        if let Ok(mut held) = held().lock() {
            held.entry(now).or_default().push(self.stream.clone());
        }
    }

    fn deadline(&mut self, timeout: NextTimeout, reading: bool) -> io::Result<()> {
        let asked = timeout.not_zero().map(|when| *when);
        let previous = if reading { self.reading } else { self.writing };
        if asked == previous {
            return Ok(());
        }
        if reading {
            self.stream.set_read_timeout(asked)?;
            self.reading = asked;
        } else {
            self.stream.set_write_timeout(asked)?;
            self.writing = asked;
        }
        Ok(())
    }
}

impl Transport for Socket {
    fn buffers(&mut self) -> &mut dyn Buffers {
        &mut self.buffers
    }

    fn transmit_output(&mut self, amount: usize, timeout: NextTimeout) -> Result<(), Error> {
        self.filed();
        self.deadline(timeout, false)?;
        let out = &self.buffers.output()[..amount];
        match (&*self.stream).write_all(out) {
            Ok(()) => Ok(()),
            Err(e) if timed_out(&e) => Err(Error::Timeout(timeout.reason)),
            Err(e) => Err(Error::Io(e)),
        }
    }

    fn await_input(&mut self, timeout: NextTimeout) -> Result<bool, Error> {
        self.filed();
        self.deadline(timeout, true)?;
        let into = self.buffers.input_append_buf();
        let read = match (&*self.stream).read(into) {
            Ok(read) => read,
            Err(e) if timed_out(&e) => return Err(Error::Timeout(timeout.reason)),
            Err(e) => return Err(Error::Io(e)),
        };
        self.buffers.input_appended(read);
        Ok(read > 0)
    }

    /// Whether this is worth keeping for the next call, which is the
    /// client's question before it takes one out of its pool.
    ///
    /// A socket with bytes waiting on it is one the server said
    /// something on that nobody asked for, and a socket that errors is
    /// closed, including the one an abort closed.
    fn is_open(&mut self) -> bool {
        let mut stream = &*self.stream;
        if stream.set_nonblocking(true).is_err() {
            return false;
        }
        let mut byte = [0];
        let quiet =
            matches!(stream.read(&mut byte), Err(e) if e.kind() == io::ErrorKind::WouldBlock);
        quiet && stream.set_nonblocking(false).is_ok()
    }
}

impl fmt::Debug for Socket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Socket")
            .field("addr", &self.stream.peer_addr().ok())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::net::SocketAddr;

    use super::first;

    fn addrs() -> Vec<SocketAddr> {
        vec![
            "[2606:4700:20::ac43:46de]:443".parse().unwrap(),
            "172.67.70.222:443".parse().unwrap(),
        ]
    }

    /// The box the examples corpus was measured on: a v6 default route
    /// with nothing behind it, and esm.sh's v6 address first. Every
    /// function answered 500 and every one of them could have been
    /// served off the second address. See #632.
    #[test]
    fn an_address_this_box_cannot_reach_is_not_the_end_of_the_list() {
        let mut tried = Vec::new();
        let opened = first(&addrs(), |addr| {
            tried.push(addr);
            match addr.is_ipv6() {
                true => Err(io::Error::from_raw_os_error(101)),
                false => Ok("a socket"),
            }
        });
        assert_eq!(opened.unwrap(), "a socket");
        assert_eq!(tried, addrs(), "both, in the order they were given");
    }

    /// A route that blackholes rather than refuses is the same bug
    /// wearing slower clothes, so an address that ran out of its share
    /// of the budget is not the end of the list either. What bounds the
    /// share is `dial`, which divides the call's budget by the number
    /// of addresses before the walk starts.
    #[test]
    fn an_address_that_ran_out_of_its_share_is_not_the_end_of_it_either() {
        let mut tried = Vec::new();
        let opened = first(&addrs(), |addr| {
            tried.push(addr);
            match addr.is_ipv6() {
                true => Err(io::Error::from(io::ErrorKind::TimedOut)),
                false => Ok("a socket"),
            }
        });
        assert_eq!(opened.unwrap(), "a socket");
        assert_eq!(tried, addrs());
    }

    /// And when none of them worked, what comes back is what the last
    /// one said rather than a sentence nobody's network produced.
    #[test]
    fn the_reason_reported_is_the_last_real_one() {
        let refused = first(&addrs(), |addr| match addr.is_ipv6() {
            true => Err::<(), _>(io::Error::from(io::ErrorKind::ConnectionRefused)),
            false => Err(io::Error::from_raw_os_error(101)),
        });
        assert_eq!(refused.unwrap_err().raw_os_error(), Some(101));
    }
}
