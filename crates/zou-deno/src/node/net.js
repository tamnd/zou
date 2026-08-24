// node:net, which is a socket with node's names on it.
//
// The socket underneath is the one this runtime already has:
// `Deno.connect`, which a database driver opens directly and which a
// package written for node reaches through this module instead. So
// what is here is a translation and not a new capability, and the line
// it draws is the same one: tcp to somewhere on the network, and not a
// unix socket, which is a file on the host.
//
// A socket is a duplex. Bytes read off the connection are pushed into
// the readable half as they arrive, and what is written to the
// writable half goes out, with the writes made before the connection
// finished opening held until it has. `end` is a half close rather
// than a hang up, which is what it is in node too: the other end may
// still have things to say and they are still readable.
//
// The server half is not here, for the reason `node:http`'s is not: a
// function is answered on the socket the server that called it owns,
// and a port of its own is not something this process gives out.
// `createServer` gives back a server that refuses when it is asked to
// listen, so a package that builds one in a branch nobody takes still
// loads.

import { Duplex } from "node:stream";
import { Buffer } from "node:buffer";

/// How much one read asks the connection for. The same number the
/// platform's own reader uses, because it is the same read.
const CHUNK = 64 * 1024;

export class Socket extends Duplex {
  #conn = null;
  #waiting = [];
  #closed = false;
  #timeout = null;
  #after = 0;

  constructor(options = {}) {
    super(options);
    this.connecting = false;
    this.pending = true;
    this.bytesRead = 0;
    this.bytesWritten = 0;
    this.remoteAddress = undefined;
    this.remotePort = undefined;
    this.localAddress = undefined;
    this.localPort = undefined;
  }

  connect(...args) {
    const { options, callback } = asked(args);
    if (typeof callback === "function") this.once("connect", callback);
    this.connecting = true;
    this.#open(options);
    return this;
  }

  async #open(options) {
    try {
      const conn = await Deno.connect({ hostname: options.host, port: options.port });
      if (this.#closed) {
        conn.close();
        return;
      }
      this.#conn = conn;
      this.connecting = false;
      this.pending = false;
      this.remoteAddress = conn.remoteAddr?.hostname;
      this.remotePort = conn.remoteAddr?.port;
      this.localAddress = conn.localAddr?.hostname;
      this.localPort = conn.localAddr?.port;
      this.emit("connect");
      this.emit("ready");
      // Whatever was written before there was anywhere to write it.
      for (const held of this.#waiting.splice(0)) held();
      this.#pump();
    } catch (why) {
      this.connecting = false;
      this.emit("error", why);
      this.#shut();
    }
  }

  /// The read loop, which is what makes a socket a stream: bytes are
  /// read as they arrive rather than when somebody asks, because a
  /// `data` listener is how node hands them over.
  async #pump() {
    const conn = this.#conn;
    try {
      for (;;) {
        const into = new Uint8Array(CHUNK);
        const read = await conn.read(into);
        if (read === null) {
          this.push(null);
          this.emit("end");
          this.#shut();
          return;
        }
        this.bytesRead += read;
        this.#stir();
        this.push(Buffer.from(into.subarray(0, read)));
      }
    } catch (why) {
      if (this.#closed) return;
      this.emit("error", why);
      this.#shut();
    }
  }

  _write(chunk, encoding, back) {
    const bytes = typeof chunk === "string"
      ? Buffer.from(chunk, encoding === "buffer" || encoding === undefined ? "utf8" : encoding)
      : Buffer.from(chunk);
    const send = () => {
      const conn = this.#conn;
      if (conn === null) {
        back(new Error("this socket is not open"));
        return;
      }
      let sent = 0;
      const more = () => {
        conn
          .write(bytes.subarray(sent))
          .then((wrote) => {
            sent += wrote;
            this.bytesWritten += wrote;
            this.#stir();
            if (sent < bytes.length) more();
            else back();
          })
          .catch(back);
      };
      more();
    };
    if (this.#conn === null) this.#waiting.push(send);
    else send();
  }

  /// This end has nothing more to say. Half a close, not a hang up.
  _final(back) {
    const done = () => {
      try {
        this.#conn?.closeWrite();
      } catch (why) {
        back(why);
        return;
      }
      back();
    };
    if (this.#conn === null && this.connecting) this.#waiting.push(done);
    else done();
  }

  /// The hang up, which the readable half calls on the way down: it
  /// is what emits `close`, and this is the piece of it that is a
  /// socket.
  _destroy(why, back) {
    this.#let_go();
    back(why);
  }

  #shut() {
    if (this.#closed) return;
    this.destroy();
  }

  #let_go() {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#timeout !== null) clearTimeout(this.#timeout);
    try {
      this.#conn?.close();
    } catch {
      // A connection the other end already hung up on is closed
      // either way, and there is nobody to tell.
    }
    this.#conn = null;
  }

  /// Node's idle timer, which fires when nothing has been read or
  /// written for a while and does not close anything on its own.
  setTimeout(after, back) {
    this.#after = Number(after) || 0;
    if (typeof back === "function") this.once("timeout", back);
    this.#stir();
    return this;
  }

  #stir() {
    if (this.#timeout !== null) clearTimeout(this.#timeout);
    if (this.#after <= 0) return;
    this.#timeout = setTimeout(() => this.emit("timeout"), this.#after);
  }

  setNoDelay() {
    return this;
  }

  setKeepAlive() {
    return this;
  }

  ref() {
    return this;
  }

  unref() {
    return this;
  }

  address() {
    if (this.localAddress === undefined) return {};
    return {
      address: this.localAddress,
      port: this.localPort,
      family: this.localAddress.includes(":") ? "IPv6" : "IPv4",
    };
  }
}

/// The arguments, which node takes as a port and an optional host, or
/// as an options object, either of them with a callback at the end.
function asked(args) {
  let callback;
  let options = {};
  for (const arg of args) {
    if (typeof arg === "function") callback = arg;
    else if (typeof arg === "number" || (typeof arg === "string" && /^\d+$/.test(arg))) {
      options.port = Number(arg);
    } else if (typeof arg === "string") options.host = arg;
    else if (arg !== null && typeof arg === "object") options = { ...options, ...arg };
  }
  if (options.path !== undefined) {
    throw new TypeError(
      "node:net may only open a tcp connection, and a path is a unix socket, which is a file on the host",
    );
  }
  options.host = options.host ?? "localhost";
  return { options, callback };
}

export function createConnection(...args) {
  const socket = new Socket();
  return socket.connect(...args);
}

export const connect = createConnection;

export class Server {
  constructor(options, handler) {
    this.listening = false;
    this.handler = typeof options === "function" ? options : handler;
  }

  listen() {
    throw new Error(
      "a function is answered on the server's own socket, so node:net createServer has no port to listen on",
    );
  }

  close(back) {
    if (typeof back === "function") queueMicrotask(back);
    return this;
  }

  address() {
    return null;
  }

  on() {
    return this;
  }

  once() {
    return this;
  }
}

export function createServer(options, handler) {
  return new Server(options, handler);
}

const V4 = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/;

export function isIPv4(value) {
  const parts = V4.exec(String(value));
  return parts !== null && parts.slice(1).every((part) => Number(part) <= 255 && String(Number(part)) === part);
}

export function isIPv6(value) {
  const said = String(value);
  if (!said.includes(":")) return false;
  // One `::` at most, and every group is up to four hex digits, with
  // the last one allowed to be a v4 address.
  const halves = said.split("::");
  if (halves.length > 2) return false;
  const groups = halves.flatMap((half) => (half === "" ? [] : half.split(":")));
  if (groups.length === 0) return halves.length === 2;
  const last = groups[groups.length - 1];
  const rest = isIPv4(last) ? groups.slice(0, -1) : groups;
  if (!rest.every((group) => /^[0-9a-fA-F]{1,4}$/.test(group))) return false;
  const size = rest.length + (isIPv4(last) ? 2 : 0);
  return halves.length === 2 ? size <= 8 : size === 8;
}

export function isIP(value) {
  if (isIPv4(value)) return 4;
  if (isIPv6(value)) return 6;
  return 0;
}

export default {
  Socket,
  Server,
  connect,
  createConnection,
  createServer,
  isIP,
  isIPv4,
  isIPv6,
};
