// node:tls, which is node:net with a handshake in front of the first
// byte.
//
// The socket underneath is the same one, `Deno.connect` with tls on it,
// so everything a package knows about a node socket is still true here:
// the same events, the same duplex, the same half close. What this
// module adds is the handshake and the few names a package reads off a
// socket once it has one, `encrypted`, `authorized`, `servername`.
//
// Two things are worth knowing before reading further.
//
// The certificate is checked, always. This runtime has one trust store,
// the host's, and `rejectUnauthorized: false` does not turn the check
// off: a function that has to talk to a server holding a certificate
// nobody signed for hands that certificate over as `ca` instead. A
// handshake that fails on a connection that asked not to be checked
// says so rather than leaving the caller to wonder why an option they
// set was not honoured.
//
// TLS goes on at connect time and not after. Node lets a package open a
// plain socket, talk on it, and upgrade it in place, which is what
// STARTTLS is, and the socket here is already being read from by the
// time anybody could ask: the read loop is what makes it a stream.
// `Deno.startTls` is in this runtime for a driver that owns its own
// connection and does the upgrade itself, and reaching it through a
// node socket would mean taking a read apart mid flight. So the upgrade
// is refused by name, with the sentence saying which call to use.

import { Socket, Server as NetServer } from "node:net";

/// The certificates a connection is to trust on top of the host's own,
/// which node takes as `ca` and this hands to the runtime as text.
///
/// One or many, a string or the bytes of one, and a secure context is
/// unwrapped on the way past because a package that made one earlier
/// passes it in place of the options it was made from.
function certs(options) {
  const given = options.ca ?? options.secureContext?.ca;
  if (given === undefined || given === null) return [];
  const many = Array.isArray(given) ? given : [given];
  return many.map((one) => (typeof one === "string" ? one : new TextDecoder().decode(one)));
}

export class TLSSocket extends Socket {
  #ca = [];
  #checked = true;

  constructor(socket, options = {}) {
    super(options);
    if (socket !== undefined && socket !== null) {
      throw new TypeError(
        "node:tls here puts tls on at connect time, so a socket that is already open cannot be upgraded: connect with tls.connect, or hold the connection yourself and use Deno.startTls",
      );
    }
    this.encrypted = true;
    this.authorized = false;
    this.authorizationError = null;
    // No protocol was negotiated by name. The handshake picks one and
    // the runtime does not say which, so this is false rather than a
    // guess a package would print.
    this.alpnProtocol = false;
    this.servername = options.servername ?? null;
    this.#ca = certs(options);
    this.#checked = options.rejectUnauthorized !== false;
    // The handshake is done by the time there is a connection at all,
    // which is what `authorized` means and what `secureConnect` is the
    // event for.
    this.on("connect", () => {
      this.authorized = true;
      this.emit("secureConnect");
    });
  }

  async _open(options) {
    const hostname = options.servername ?? this.servername ?? options.host;
    const ca = certs(options).concat(this.#ca);
    const checked = options.rejectUnauthorized !== false && this.#checked;
    this.servername = hostname;
    try {
      return await Deno.connectTls({ hostname, port: options.port, caCerts: ca });
    } catch (why) {
      if (checked) throw why;
      const wrong = new Error(
        `${why.message}: node:tls here always checks the certificate, and rejectUnauthorized false does not turn that off. Pass the certificate as ca instead.`,
      );
      wrong.cause = why;
      wrong.code = why.code;
      throw wrong;
    }
  }

  /// What the other end presented. The handshake happens inside the
  /// host and the certificate does not come back out of it, so this is
  /// empty rather than invented: a package that reads a field off it
  /// gets undefined, which is what node gives for a socket that has no
  /// peer certificate to show.
  getPeerCertificate() {
    return {};
  }

  getCertificate() {
    return null;
  }

  /// The version and the cipher the handshake settled on, which the
  /// runtime does not report back out of the host. Null is node's own
  /// answer for a socket with no protocol to name, and it is a better
  /// one than a version this did not negotiate.
  getProtocol() {
    return null;
  }

  getCipher() {
    return {};
  }

  getSession() {
    return undefined;
  }

  isSessionReused() {
    return false;
  }

  setServername(name) {
    this.servername = name;
    return this;
  }

  disableRenegotiation() {}
}

export function connect(...args) {
  let callback;
  let options = {};
  for (const arg of args) {
    if (typeof arg === "function") callback = arg;
    else if (typeof arg === "number" || (typeof arg === "string" && /^\d+$/.test(arg))) {
      options.port = Number(arg);
    } else if (typeof arg === "string") options.host = arg;
    else if (arg !== null && typeof arg === "object") options = { ...options, ...arg };
  }
  if (options.socket !== undefined) {
    throw new TypeError(
      "node:tls here puts tls on at connect time, so a socket that is already open cannot be upgraded: connect with tls.connect, or hold the connection yourself and use Deno.startTls",
    );
  }
  const socket = new TLSSocket(null, options);
  // Node calls the callback back on `secureConnect` rather than on
  // `connect`, and here the two are the same moment.
  if (typeof callback === "function") socket.once("secureConnect", callback);
  socket.connect(options);
  return socket;
}

/// The options, kept. Node compiles them into a context the handshake
/// reads and hands back an opaque thing; a package makes one early and
/// passes it to `connect` later, which is the whole of what it is used
/// for here.
export class SecureContext {
  constructor(options = {}) {
    this.options = options;
    this.ca = options.ca;
  }
}

export function createSecureContext(options = {}) {
  return new SecureContext(options);
}

/// The check node runs after a handshake, which the host has already
/// run: a certificate for the wrong name never gets this far. Node
/// answers with an error or with undefined, and undefined is what a
/// connection that exists has earned.
export function checkServerIdentity() {
  return undefined;
}

/// The trust store belongs to the host, so there is no list of it to
/// hand out. Empty rather than absent, because a package that maps over
/// it wants an array.
export const rootCertificates = Object.freeze([]);

export const DEFAULT_MIN_VERSION = "TLSv1.2";
export const DEFAULT_MAX_VERSION = "TLSv1.3";
export const DEFAULT_ECDH_CURVE = "auto";

export class Server extends NetServer {
  addContext() {
    return this;
  }

  setSecureContext() {
    return this;
  }
}

/// The server half, which refuses at `listen` for the reason node:net's
/// does: a function is answered on the socket the server that called it
/// owns, and a port of its own is not something this process gives out.
export function createServer(options, handler) {
  return new Server(options, handler);
}

export const createConnection = connect;

export default {
  DEFAULT_ECDH_CURVE,
  DEFAULT_MAX_VERSION,
  DEFAULT_MIN_VERSION,
  SecureContext,
  Server,
  TLSSocket,
  checkServerIdentity,
  connect,
  createConnection,
  createSecureContext,
  createServer,
  rootCertificates,
};
