// node:http, which here is the client half and says so about the other
// one.
//
// A package reaching for this is almost always making a request, and a
// request in this runtime is a `fetch`, so that is what is underneath:
// `http.request` builds a `Request` out of the options node's api
// takes, sends it when the caller ends the body, and hands back the
// response as a readable with node's names on it.
//
// The server half is not here and is not missing by accident. A
// function is called by a server that already owns the socket, and
// `Deno.serve` is how it answers on that one. A second server, on a
// port of its own, is a thing this process will not give out, so
// `createServer` returns a server that refuses when it is asked to
// listen. That is the shape rather than a refused import for the same
// reason `child_process` is a module and not a missing one: a package
// that builds a server in a branch nobody takes runs fine against
// this and does not load at all against nothing.
//
// What a client loses against node's is the socket underneath it.
// There is no `agent` with a pool a caller can size, no `socket` event
// with a real socket on it, no continuation and no upgrade, because
// the connection belongs to the host's http client. `Agent` exists,
// holds what it was given and is otherwise ignored.

import { Readable, Writable } from "node:stream";
import { Buffer } from "node:buffer";

export const METHODS = [
  "ACL", "BIND", "CHECKOUT", "CONNECT", "COPY", "DELETE", "GET", "HEAD",
  "LINK", "LOCK", "M-SEARCH", "MERGE", "MKACTIVITY", "MKCALENDAR", "MKCOL",
  "MOVE", "NOTIFY", "OPTIONS", "PATCH", "POST", "PROPFIND", "PROPPATCH",
  "PURGE", "PUT", "REBIND", "REPORT", "SEARCH", "SOURCE", "SUBSCRIBE",
  "TRACE", "UNBIND", "UNLINK", "UNLOCK", "UNSUBSCRIBE",
];

export const STATUS_CODES = {
  100: "Continue",
  101: "Switching Protocols",
  102: "Processing",
  103: "Early Hints",
  200: "OK",
  201: "Created",
  202: "Accepted",
  203: "Non-Authoritative Information",
  204: "No Content",
  205: "Reset Content",
  206: "Partial Content",
  207: "Multi-Status",
  208: "Already Reported",
  226: "IM Used",
  300: "Multiple Choices",
  301: "Moved Permanently",
  302: "Found",
  303: "See Other",
  304: "Not Modified",
  305: "Use Proxy",
  307: "Temporary Redirect",
  308: "Permanent Redirect",
  400: "Bad Request",
  401: "Unauthorized",
  402: "Payment Required",
  403: "Forbidden",
  404: "Not Found",
  405: "Method Not Allowed",
  406: "Not Acceptable",
  407: "Proxy Authentication Required",
  408: "Request Timeout",
  409: "Conflict",
  410: "Gone",
  411: "Length Required",
  412: "Precondition Failed",
  413: "Payload Too Large",
  414: "URI Too Long",
  415: "Unsupported Media Type",
  416: "Range Not Satisfiable",
  417: "Expectation Failed",
  418: "I'm a Teapot",
  421: "Misdirected Request",
  422: "Unprocessable Entity",
  423: "Locked",
  424: "Failed Dependency",
  425: "Too Early",
  426: "Upgrade Required",
  428: "Precondition Required",
  429: "Too Many Requests",
  431: "Request Header Fields Too Large",
  451: "Unavailable For Legal Reasons",
  500: "Internal Server Error",
  501: "Not Implemented",
  502: "Bad Gateway",
  503: "Service Unavailable",
  504: "Gateway Timeout",
  505: "HTTP Version Not Supported",
  506: "Variant Also Negotiates",
  507: "Insufficient Storage",
  508: "Loop Detected",
  509: "Bandwidth Limit Exceeded",
  510: "Not Extended",
  511: "Network Authentication Required",
};

/// The url a call is for, out of the three ways node lets it be said:
/// a string, a `URL`, or an options object with a host and a path on
/// it. The pieces that are missing take node's defaults.
function urlOf(options, fallback) {
  if (options.url !== undefined) return options.url;
  const protocol = options.protocol ?? fallback;
  const host = options.hostname ?? options.host ?? "localhost";
  const port = options.port === undefined || options.port === null ? "" : `:${options.port}`;
  const path = options.path ?? "/";
  const auth = options.auth === undefined ? "" : `${options.auth}@`;
  return `${protocol}//${auth}${host}${port}${path.startsWith("/") ? path : `/${path}`}`;
}

/// The arguments, which node takes in five shapes: a url, a url and
/// options, options, and any of those with a callback at the end.
function asked(first, second, third, fallback) {
  let callback;
  let options = {};
  const args = [first, second, third].filter((it) => it !== undefined);
  for (const arg of args) {
    if (typeof arg === "function") {
      callback = arg;
    } else if (typeof arg === "string" || arg instanceof URL) {
      const url = new URL(String(arg));
      options = {
        ...options,
        url: url.href,
        protocol: url.protocol,
        hostname: url.hostname,
        port: url.port,
        path: `${url.pathname}${url.search}`,
      };
    } else if (arg !== null && typeof arg === "object") {
      // A url given first and options second: the options win on the
      // pieces they mention, and the url is rebuilt from what is left.
      options = { ...options, ...arg, url: undefined };
    }
  }
  return { options: { ...options, url: options.url ?? urlOf(options, fallback) }, callback };
}

/// A response, which is a readable with node's names on it. The body
/// is the platform's stream, read into this one as it arrives rather
/// than held whole, because a caller reading a large answer a chunk at
/// a time asked for exactly that.
export class IncomingMessage extends Readable {
  constructor(response) {
    super();
    this.statusCode = response.status;
    this.statusMessage = response.statusText || STATUS_CODES[response.status] || "";
    this.httpVersion = "1.1";
    this.httpVersionMajor = 1;
    this.httpVersionMinor = 1;
    this.complete = false;
    this.headers = {};
    this.rawHeaders = [];
    for (const [name, value] of response.headers) {
      this.headers[name] = value;
      this.rawHeaders.push(name, value);
    }
    this.#read(response.body);
  }

  async #read(body) {
    if (body === null || body === undefined) {
      this.complete = true;
      this.push(null);
      return;
    }
    try {
      for await (const chunk of Readable.fromWeb(body)) {
        this.push(Buffer.from(chunk));
      }
      this.complete = true;
      this.push(null);
    } catch (why) {
      this.destroy(why);
    }
  }

  setTimeout(after, back) {
    if (typeof back === "function") this.on("timeout", back);
    return this;
  }
}

/// A request, which is a writable: what is written to it is the body,
/// and ending it is what sends the call.
export class ClientRequest extends Writable {
  #options;
  #headers = new Map();
  #body = [];
  #sent = false;
  #stop = new AbortController();

  constructor(options, callback) {
    super();
    this.#options = options;
    this.method = (options.method ?? "GET").toUpperCase();
    this.path = options.path ?? "/";
    this.host = options.hostname ?? options.host ?? "localhost";
    this.protocol = options.protocol ?? "http:";
    this.finished = false;
    for (const [name, value] of Object.entries(options.headers ?? {})) {
      if (value !== undefined && value !== null) this.#headers.set(String(name).toLowerCase(), value);
    }
    if (typeof callback === "function") this.once("response", callback);
    if (options.signal !== undefined && options.signal !== null) {
      options.signal.addEventListener("abort", () => this.destroy(options.signal.reason));
    }
  }

  setHeader(name, value) {
    this.#headers.set(String(name).toLowerCase(), value);
    return this;
  }

  getHeader(name) {
    return this.#headers.get(String(name).toLowerCase());
  }

  getHeaders() {
    return Object.fromEntries(this.#headers);
  }

  removeHeader(name) {
    this.#headers.delete(String(name).toLowerCase());
  }

  hasHeader(name) {
    return this.#headers.has(String(name).toLowerCase());
  }

  _write(chunk, encoding, back) {
    this.#body.push(typeof chunk === "string" ? Buffer.from(chunk, encoding === "buffer" ? undefined : encoding) : Buffer.from(chunk));
    back();
  }

  _final(back) {
    back();
    this.#send();
  }

  async #send() {
    if (this.#sent) return;
    this.#sent = true;
    this.finished = true;
    const headers = new Headers();
    for (const [name, value] of this.#headers) {
      // Node lets a header be a list, and each of them is its own line
      // on the wire.
      for (const one of Array.isArray(value) ? value : [value]) {
        headers.append(name, String(one));
      }
    }
    const carries = this.method !== "GET" && this.method !== "HEAD" && this.#body.length > 0;
    try {
      const response = await fetch(this.#options.url, {
        method: this.method,
        headers,
        body: carries ? Buffer.concat(this.#body) : undefined,
        signal: this.#stop.signal,
        redirect: "manual",
      });
      const message = new IncomingMessage(response);
      this.res = message;
      this.emit("response", message);
    } catch (why) {
      // An abort is not a failure to report twice: `destroy` has
      // already said so.
      if (this.#stop.signal.aborted) return;
      this.emit("error", why);
    }
  }

  /// Node's old name for it and the one a package still calls.
  abort() {
    this.destroy();
  }

  destroy(why) {
    this.#stop.abort(why);
    if (why !== undefined && why !== null) this.emit("error", why);
    this.emit("close");
    return this;
  }

  setTimeout(after, back) {
    if (typeof back === "function") this.on("timeout", back);
    return this;
  }

  setNoDelay() {}
  setSocketKeepAlive() {}
  flushHeaders() {}
}

/// A server, which cannot listen. The object is here so a package that
/// builds one in a branch nobody takes still loads, and the refusal is
/// at the call that would need a port of its own.
export class Server {
  constructor(options, handler) {
    this.listening = false;
    this.handler = typeof options === "function" ? options : handler;
  }

  listen() {
    throw new Error(
      "a function is answered on the server's own socket, so node:http createServer has no port to listen on",
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

  setTimeout() {
    return this;
  }
}

export function createServer(options, handler) {
  return new Server(options, handler);
}

/// The pool node keeps its sockets in, which the host's http client
/// keeps instead. It holds what it was handed so a caller reading its
/// own options back finds them.
export class Agent {
  constructor(options = {}) {
    this.options = options;
    this.maxSockets = options.maxSockets ?? Infinity;
    this.maxFreeSockets = options.maxFreeSockets ?? 256;
    this.keepAlive = options.keepAlive ?? false;
    this.sockets = {};
    this.freeSockets = {};
    this.requests = {};
  }

  destroy() {}
}

export const globalAgent = new Agent({ keepAlive: true });

export function request(first, second, third) {
  const { options, callback } = asked(first, second, third, "http:");
  return new ClientRequest(options, callback);
}

export function get(first, second, third) {
  const made = request(first, second, third);
  made.end();
  return made;
}

/// Node's own name for a response being written by a server, which
/// there is no server to write. It is here because a package reads the
/// class off the module to check a value against it.
export class ServerResponse extends Writable {}
export class OutgoingMessage extends Writable {}

export const maxHeaderSize = 16384;

export function setMaxIdleHTTPParsers() {}

export default {
  METHODS,
  STATUS_CODES,
  Agent,
  ClientRequest,
  IncomingMessage,
  OutgoingMessage,
  Server,
  ServerResponse,
  createServer,
  get,
  globalAgent,
  maxHeaderSize,
  request,
  setMaxIdleHTTPParsers,
};
