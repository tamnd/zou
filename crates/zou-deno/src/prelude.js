// The shape of the web a Deno handler is written against, as much of it
// as running one function needs and no more.
//
// Deno's own Headers, Request and Response live in deno_fetch, which
// arrives with a whole HTTP client and a second TLS stack behind it.
// That is a real dependency to take and it is worth taking on purpose
// rather than by accident, so for now these are written here and the
// gaps are listed in the crate docs rather than discovered by whoever
// deploys a function.
//
// Nothing here reaches the global scope except what a handler is
// entitled to see. The last expression is the entry point, so the host
// holds it and no user code can reach it, rename it or shadow it.

"use strict";

((globalThis) => {
  const core = Deno.core;
  const ops = core.ops;

  // The one piece of state the module leaves behind, and the reason the
  // whole file is a closure: `Deno.serve(handler)` puts the handler
  // here and `run` reads it, with nowhere else for either to look.
  let handler = null;
  let served = false;

  // ---------------------------------------------------------------
  // Bytes and text

  class TextEncoder {
    get encoding() {
      return "utf-8";
    }
    encode(input = "") {
      return core.encode(String(input));
    }
    encodeInto(input, dest) {
      const bytes = core.encode(String(input));
      const written = Math.min(bytes.length, dest.length);
      dest.set(bytes.subarray(0, written));
      // Not the spec's answer when a surrogate pair straddles the end
      // of the buffer, which is why this is in the gap list.
      return { read: input.length, written };
    }
  }

  class TextDecoder {
    constructor(label = "utf-8") {
      const encoding = String(label).toLowerCase();
      if (encoding !== "utf-8" && encoding !== "utf8" && encoding !== "unicode-1-1-utf-8") {
        throw new RangeError(`the encoding label provided ('${label}') is not supported`);
      }
    }
    get encoding() {
      return "utf-8";
    }
    decode(input) {
      if (input === undefined) {
        return "";
      }
      return core.decode(bytesOf(input));
    }
  }

  const BASE64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

  function btoa(input) {
    const s = String(input);
    let out = "";
    for (let i = 0; i < s.length; i += 3) {
      const a = s.charCodeAt(i);
      const b = i + 1 < s.length ? s.charCodeAt(i + 1) : NaN;
      const c = i + 2 < s.length ? s.charCodeAt(i + 2) : NaN;
      if (a > 255 || b > 255 || c > 255) {
        throw new TypeError("The string to be encoded contains characters outside of the Latin1 range.");
      }
      const n = (a << 16) | ((b || 0) << 8) | (c || 0);
      out += BASE64[(n >> 18) & 63] + BASE64[(n >> 12) & 63];
      out += Number.isNaN(b) ? "=" : BASE64[(n >> 6) & 63];
      out += Number.isNaN(c) ? "=" : BASE64[n & 63];
    }
    return out;
  }

  function atob(input) {
    const s = String(input).replace(/[\t\n\f\r ]/g, "");
    if (s.length % 4 === 1) {
      throw new TypeError("The string to be decoded is not correctly encoded.");
    }
    const trimmed = s.replace(/=+$/, "");
    let out = "";
    let bits = 0;
    let held = 0;
    for (const ch of trimmed) {
      const v = BASE64.indexOf(ch);
      if (v < 0) {
        throw new TypeError("The string to be decoded is not correctly encoded.");
      }
      held = (held << 6) | v;
      bits += 6;
      if (bits >= 8) {
        bits -= 8;
        out += String.fromCharCode((held >> bits) & 255);
      }
    }
    return out;
  }

  const encoder = new TextEncoder();

  /// Whatever a caller handed us, as the bytes of it.
  function bytesOf(value) {
    if (value instanceof Uint8Array) {
      return value;
    }
    if (value instanceof ArrayBuffer) {
      return new Uint8Array(value);
    }
    if (ArrayBuffer.isView(value)) {
      return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    }
    return encoder.encode(String(value));
  }

  // ---------------------------------------------------------------
  // Headers

  const HEADERS = Symbol("headers");

  function headerName(name) {
    const lowered = String(name).toLowerCase();
    if (lowered === "" || /[^!#$%&'*+\-.^_`|~0-9a-z]/.test(lowered)) {
      throw new TypeError(`Invalid header name: "${name}"`);
    }
    return lowered;
  }

  function headerValue(value) {
    return String(value).replace(/^[\t\n\r ]+|[\t\n\r ]+$/g, "");
  }

  class Headers {
    constructor(init) {
      this[HEADERS] = [];
      if (init === undefined || init === null) {
        return;
      }
      if (init instanceof Headers) {
        for (const [name, value] of init[HEADERS]) {
          this[HEADERS].push([name, value]);
        }
      } else if (Array.isArray(init)) {
        for (const pair of init) {
          if (!Array.isArray(pair) || pair.length !== 2) {
            throw new TypeError("Headers init must be a list of name and value pairs");
          }
          this.append(pair[0], pair[1]);
        }
      } else {
        for (const name of Object.keys(init)) {
          this.append(name, init[name]);
        }
      }
    }

    append(name, value) {
      this[HEADERS].push([headerName(name), headerValue(value)]);
    }

    set(name, value) {
      const lowered = headerName(name);
      const kept = this[HEADERS].filter((pair) => pair[0] !== lowered);
      kept.push([lowered, headerValue(value)]);
      this[HEADERS] = kept;
    }

    // Every value under the name, joined, which is what the spec says
    // and what a caller reading a repeated header expects.
    get(name) {
      const lowered = headerName(name);
      const found = this[HEADERS].filter((pair) => pair[0] === lowered);
      return found.length === 0 ? null : found.map((pair) => pair[1]).join(", ");
    }

    has(name) {
      const lowered = headerName(name);
      return this[HEADERS].some((pair) => pair[0] === lowered);
    }

    delete(name) {
      const lowered = headerName(name);
      this[HEADERS] = this[HEADERS].filter((pair) => pair[0] !== lowered);
    }

    *entries() {
      // Sorted by name, which is what iterating a Headers gives in every
      // browser and in Deno.
      const sorted = this[HEADERS].slice().sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
      for (const [name, value] of sorted) {
        yield [name, value];
      }
    }

    *keys() {
      for (const [name] of this.entries()) {
        yield name;
      }
    }

    *values() {
      for (const [, value] of this.entries()) {
        yield value;
      }
    }

    forEach(fn, thisArg) {
      for (const [name, value] of this.entries()) {
        fn.call(thisArg, value, name, this);
      }
    }

    [Symbol.iterator]() {
      return this.entries();
    }
  }

  // ---------------------------------------------------------------
  // Bodies

  const BODY = Symbol("body");
  const USED = Symbol("bodyUsed");

  function readBody(target) {
    if (target[USED]) {
      throw new TypeError("Body already consumed.");
    }
    target[USED] = true;
    return target[BODY];
  }

  const bodyMethods = {
    async arrayBuffer() {
      const bytes = readBody(this);
      return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
    },
    async bytes() {
      return readBody(this).slice();
    },
    async text() {
      return core.decode(readBody(this));
    },
    async json() {
      return JSON.parse(core.decode(readBody(this)));
    },
    get bodyUsed() {
      return this[USED];
    },
  };

  /// The bytes of a body, and the content type it implies when the
  /// caller did not name one.
  function intoBody(body) {
    if (body === undefined || body === null) {
      return [new Uint8Array(0), null];
    }
    if (typeof body === "string") {
      return [encoder.encode(body), "text/plain;charset=UTF-8"];
    }
    if (body instanceof Uint8Array || body instanceof ArrayBuffer || ArrayBuffer.isView(body)) {
      return [bytesOf(body), null];
    }
    if (body instanceof ReadableStreamStub) {
      throw new TypeError("a streamed body is not supported yet");
    }
    return [encoder.encode(String(body)), "text/plain;charset=UTF-8"];
  }

  // Named so the check above has something to name, and so a handler
  // asking for the class gets a clear failure rather than `undefined`.
  class ReadableStreamStub {
    constructor() {
      throw new TypeError("ReadableStream is not implemented yet");
    }
  }

  // ---------------------------------------------------------------
  // Request and Response

  class Request {
    constructor(input, init = {}) {
      if (input instanceof Request) {
        this.url = input.url;
        this.method = init.method ? String(init.method).toUpperCase() : input.method;
        this.headers = new Headers(init.headers ?? input.headers);
        this[BODY] = init.body === undefined ? input[BODY] : intoBody(init.body)[0];
      } else {
        this.url = String(input);
        this.method = init.method ? String(init.method).toUpperCase() : "GET";
        this.headers = new Headers(init.headers);
        const [bytes, type] = intoBody(init.body);
        this[BODY] = bytes;
        if (type !== null && !this.headers.has("content-type")) {
          this.headers.set("content-type", type);
        }
      }
      this[USED] = false;
      Object.defineProperty(this, "body", { value: null, enumerable: true });
    }

    clone() {
      const copy = new Request(this.url, { method: this.method, headers: this.headers });
      copy[BODY] = this[BODY];
      return copy;
    }
  }
  Object.assign(Request.prototype, bodyMethods);

  const REDIRECTS = [301, 302, 303, 307, 308];
  const NO_BODY = [101, 204, 205, 304];

  class Response {
    constructor(body, init = {}) {
      const status = init.status === undefined ? 200 : Number(init.status);
      if (!Number.isInteger(status) || status < 200 || status > 599) {
        throw new RangeError(`The status provided (${init.status}) is outside the range [200, 599].`);
      }
      this.status = status;
      this.statusText = init.statusText === undefined ? "" : String(init.statusText);
      this.headers = new Headers(init.headers);
      if (NO_BODY.includes(status) && body !== undefined && body !== null) {
        throw new TypeError("Response with null body status cannot have body");
      }
      const [bytes, type] = intoBody(body);
      this[BODY] = bytes;
      this[USED] = false;
      if (type !== null && !this.headers.has("content-type")) {
        this.headers.set("content-type", type);
      }
      Object.defineProperty(this, "body", { value: null, enumerable: true });
      // A response nobody fetched has no url, which is what Deno gives
      // back for one a handler built itself.
      this.url = "";
    }

    get ok() {
      return this.status >= 200 && this.status < 300;
    }

    get redirected() {
      return false;
    }

    get type() {
      return "default";
    }

    clone() {
      const copy = new Response(null, { status: this.status, statusText: this.statusText, headers: this.headers });
      copy[BODY] = this[BODY];
      return copy;
    }

    static json(data, init = {}) {
      const res = new Response(JSON.stringify(data), init);
      res.headers.set("content-type", "application/json");
      return res;
    }

    static redirect(url, status = 302) {
      if (!REDIRECTS.includes(status)) {
        throw new RangeError("The redirection status provided is not a redirection status.");
      }
      const res = new Response(null, { status });
      res.headers.set("location", String(url));
      return res;
    }

    static error() {
      // The spec's network error, which a handler can return and which
      // is a 500 by the time it reaches the caller.
      const res = new Response(null, { status: 500 });
      return res;
    }
  }
  Object.assign(Response.prototype, bodyMethods);

  // ---------------------------------------------------------------
  // fetch

  const FETCHABLE = ["http:", "https:"];

  /// The scheme, without a URL parser to ask, which is the same
  /// question the module loader answers for a specifier.
  function schemeOf(url) {
    const colon = url.indexOf(":");
    const scheme = colon === -1 ? "" : url.slice(0, colon + 1).toLowerCase();
    return /^[a-z][a-z0-9+\-.]*:$/.test(scheme) ? scheme : "";
  }

  /// A response that came off the wire rather than out of a handler, so
  /// the constructor's rules about status ranges and null bodies do not
  /// apply: what a server said is what the handler is told it said.
  function received(answer) {
    const res = Object.create(Response.prototype);
    res.status = answer.status;
    res.statusText = answer.statusText;
    res.headers = new Headers(answer.headers);
    res.url = answer.url;
    res[BODY] = answer.body;
    res[USED] = false;
    Object.defineProperty(res, "body", { value: null, enumerable: true });
    Object.defineProperty(res, "redirected", { value: answer.redirected });
    return res;
  }

  async function fetch(input, init) {
    const request = input instanceof Request && init === undefined ? input : new Request(input, init ?? {});
    const scheme = schemeOf(request.url);
    if (scheme === "") {
      throw new TypeError(`Invalid URL: '${request.url}'`);
    }
    if (!FETCHABLE.includes(scheme)) {
      throw new TypeError(`fetch does not serve the ${scheme.slice(0, -1)} scheme yet`);
    }
    // Reading the body is what sending it is, and a request whose body
    // was already read is a request with nothing left to send.
    const body = readBody(request);
    const answer = await ops.op_zou_fetch(
      {
        method: request.method,
        url: request.url,
        headers: Array.from(request.headers.entries()),
      },
      body,
    );
    return received(answer);
  }

  // ---------------------------------------------------------------
  // console

  function shown(value) {
    if (typeof value === "string") {
      return value;
    }
    if (value instanceof Error) {
      return value.stack ?? `${value.name}: ${value.message}`;
    }
    try {
      return JSON.stringify(value) ?? String(value);
    } catch {
      return String(value);
    }
  }

  function printer(toStderr) {
    return (...args) => core.print(`${args.map(shown).join(" ")}\n`, toStderr);
  }

  const console = {
    log: printer(false),
    info: printer(false),
    debug: printer(false),
    dir: printer(false),
    trace: printer(true),
    warn: printer(true),
    error: printer(true),
  };

  // ---------------------------------------------------------------
  // Deno.serve and Deno.env

  function serve(first, second) {
    if (served) {
      throw new TypeError("Deno.serve was called twice in one function");
    }
    let fn = null;
    if (typeof first === "function") {
      fn = first;
    } else if (typeof second === "function") {
      fn = second;
    } else if (first !== null && typeof first === "object" && typeof first.handler === "function") {
      fn = first.handler;
    }
    if (fn === null) {
      throw new TypeError("Deno.serve requires a handler");
    }
    handler = fn;
    served = true;
    // What upstream hands back is a server with a shutdown and a
    // promise that settles when it is done. Here the process holding
    // the socket is the host, so the promise never settles and
    // shutdown is the host's business.
    return {
      finished: new Promise(() => {}),
      addr: { transport: "tcp", hostname: "0.0.0.0", port: 9000 },
      shutdown() {
        return Promise.resolve();
      },
      ref() {},
      unref() {},
    };
  }

  const env = {
    get(name) {
      const found = ops.op_zou_env_get(String(name));
      return found === null ? undefined : found;
    },
    has(name) {
      return ops.op_zou_env_get(String(name)) !== null;
    },
    set() {
      throw new TypeError("the environment of a function is read only");
    },
    delete() {
      throw new TypeError("the environment of a function is read only");
    },
    toObject() {
      return ops.op_zou_env();
    },
  };

  // ---------------------------------------------------------------
  // What the module sees

  Object.assign(globalThis, {
    Headers,
    Request,
    Response,
    TextDecoder,
    TextEncoder,
    ReadableStream: ReadableStreamStub,
    atob,
    btoa,
    console,
    fetch,
  });
  globalThis.self = globalThis;
  globalThis.window = undefined;

  Object.assign(Deno, {
    serve,
    env,
    // Enough of it that a function branching on the platform gets an
    // answer rather than an exception.
    build: { target: "unknown", arch: "unknown", os: "linux", vendor: "unknown" },
    version: { deno: "zou", v8: "", typescript: "" },
    exit() {
      throw new TypeError("a function may not exit the process it is running in");
    },
  });

  // ---------------------------------------------------------------
  // The entry point, which is the value of this whole file

  async function run() {
    if (handler === null) {
      throw new TypeError("the function did not call Deno.serve");
    }
    const call = ops.op_zou_call();
    const request = new Request(call.url, {
      method: call.method,
      headers: call.headers,
      body: call.method === "GET" || call.method === "HEAD" ? undefined : call.body,
    });
    const answer = await handler(request, {
      remoteAddr: { transport: "tcp", hostname: call.peer, port: 0 },
    });
    if (!(answer instanceof Response)) {
      throw new TypeError("a handler must return a Response");
    }
    ops.op_zou_answer(answer.status, Array.from(answer.headers.entries()), answer[BODY]);
  }

  return run;
})(globalThis);
