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
  // URL and URLSearchParams
  //
  // The parsing is a pair of ops, because the crate that parses urls is
  // already in this build and a url parser written here would be a few
  // hundred lines that is wrong in the corners. What is here is the
  // shape: properties that read a parsed result, setters that ask for
  // one component to be changed, and the query string, which is its own
  // small format and has nothing to do with the parser.

  const PARTS = Symbol("parts");
  const PARAMS = Symbol("searchParams");
  const OWNER = Symbol("owner");
  const PAIRS = Symbol("pairs");

  const COMPONENTS = [
    "href",
    "protocol",
    "username",
    "password",
    "host",
    "hostname",
    "port",
    "pathname",
    "search",
    "hash",
  ];

  /// x-www-form-urlencoded, which is not `encodeURIComponent`: a space
  /// is a plus here, and the set left alone is narrower.
  function urlencoded(text) {
    let out = "";
    for (const character of String(text)) {
      if (/^[A-Za-z0-9*\-._]$/.test(character)) {
        out += character;
      } else if (character === " ") {
        out += "+";
      } else {
        for (const byte of encoder.encode(character)) {
          out += "%" + byte.toString(16).toUpperCase().padStart(2, "0");
        }
      }
    }
    return out;
  }

  /// A percent sequence that is not valid utf-8 is left as it was
  /// rather than replaced, which is the one place this differs from the
  /// spec and is a difference nobody sends on purpose.
  function urldecoded(text) {
    try {
      return decodeURIComponent(String(text).replace(/\+/g, " "));
    } catch {
      return String(text);
    }
  }

  function pairsOf(query) {
    const pairs = [];
    for (const piece of String(query).replace(/^\?/, "").split("&")) {
      if (piece === "") {
        continue;
      }
      const equals = piece.indexOf("=");
      const name = equals === -1 ? piece : piece.slice(0, equals);
      const value = equals === -1 ? "" : piece.slice(equals + 1);
      pairs.push([urldecoded(name), urldecoded(value)]);
    }
    return pairs;
  }

  /// A url's `searchParams` is the url's, so changing one changes the
  /// other, in both directions.
  function wroteParams(params) {
    const url = params[OWNER];
    if (url !== null) {
      const changed = ops.op_zou_url_set(url[PARTS].href, "search", params.toString());
      if (changed !== null) {
        url[PARTS] = changed;
      }
    }
  }

  function wroteUrl(url) {
    if (url[PARAMS] !== null) {
      url[PARAMS][PAIRS] = pairsOf(url[PARTS].search);
    }
  }

  class URLSearchParams {
    constructor(init = "") {
      this[PAIRS] = [];
      this[OWNER] = null;
      if (init instanceof URLSearchParams) {
        this[PAIRS] = init[PAIRS].map(([name, value]) => [name, value]);
      } else if (Array.isArray(init)) {
        for (const pair of init) {
          if (!Array.isArray(pair) || pair.length !== 2) {
            throw new TypeError("Each query pair must be an iterable [name, value] tuple");
          }
          this[PAIRS].push([String(pair[0]), String(pair[1])]);
        }
      } else if (init !== null && typeof init === "object") {
        for (const name of Object.keys(init)) {
          this[PAIRS].push([name, String(init[name])]);
        }
      } else {
        this[PAIRS] = pairsOf(init);
      }
    }

    get size() {
      return this[PAIRS].length;
    }

    append(name, value) {
      this[PAIRS].push([String(name), String(value)]);
      wroteParams(this);
    }

    delete(name, value) {
      const wanted = String(name);
      this[PAIRS] = this[PAIRS].filter(
        ([held, kept]) => held !== wanted || (value !== undefined && kept !== String(value)),
      );
      wroteParams(this);
    }

    get(name) {
      const found = this[PAIRS].find(([held]) => held === String(name));
      return found === undefined ? null : found[1];
    }

    getAll(name) {
      return this[PAIRS].filter(([held]) => held === String(name)).map(([, value]) => value);
    }

    has(name, value) {
      return this[PAIRS].some(
        ([held, kept]) => held === String(name) && (value === undefined || kept === String(value)),
      );
    }

    set(name, value) {
      const wanted = String(name);
      const first = this[PAIRS].findIndex(([held]) => held === wanted);
      if (first === -1) {
        this[PAIRS].push([wanted, String(value)]);
      } else {
        this[PAIRS][first] = [wanted, String(value)];
        this[PAIRS] = this[PAIRS].filter((pair, at) => at <= first || pair[0] !== wanted);
      }
      wroteParams(this);
    }

    sort() {
      // By name only, and stable, so pairs with the same name keep the
      // order they were appended in.
      this[PAIRS] = this[PAIRS]
        .map((pair, at) => [pair, at])
        .sort(([one, first], [two, second]) =>
          one[0] === two[0] ? first - second : one[0] < two[0] ? -1 : 1,
        )
        .map(([pair]) => pair);
      wroteParams(this);
    }

    forEach(callback, self) {
      for (const [name, value] of this[PAIRS].slice()) {
        callback.call(self, value, name, this);
      }
    }

    *entries() {
      yield* this[PAIRS].map(([name, value]) => [name, value]);
    }

    *keys() {
      for (const [name] of this[PAIRS]) {
        yield name;
      }
    }

    *values() {
      for (const [, value] of this[PAIRS]) {
        yield value;
      }
    }

    [Symbol.iterator]() {
      return this.entries();
    }

    toString() {
      return this[PAIRS].map(([name, value]) => `${urlencoded(name)}=${urlencoded(value)}`).join("&");
    }
  }

  class URL {
    constructor(input, base) {
      const parts = ops.op_zou_url_parse(String(input), base === undefined ? "" : String(base));
      if (parts === null) {
        throw new TypeError(`Invalid URL: '${input}'`);
      }
      this[PARTS] = parts;
      this[PARAMS] = null;
    }

    get origin() {
      return this[PARTS].origin;
    }

    get searchParams() {
      if (this[PARAMS] === null) {
        const params = new URLSearchParams(this[PARTS].search);
        params[OWNER] = this;
        this[PARAMS] = params;
      }
      return this[PARAMS];
    }

    toString() {
      return this[PARTS].href;
    }

    toJSON() {
      return this[PARTS].href;
    }

    static canParse(input, base) {
      return ops.op_zou_url_parse(String(input), base === undefined ? "" : String(base)) !== null;
    }

    static parse(input, base) {
      try {
        return new URL(input, base);
      } catch {
        return null;
      }
    }
  }

  // Ten properties that are all the same property, so they are written
  // once. A setter that the parser will not honour leaves the url as it
  // was, which is what the spec asks for and is not the same as throwing.
  for (const component of COMPONENTS) {
    Object.defineProperty(URL.prototype, component, {
      enumerable: true,
      configurable: true,
      get() {
        return this[PARTS][component];
      },
      set(value) {
        const changed = ops.op_zou_url_set(this[PARTS].href, component, String(value));
        if (changed !== null) {
          this[PARTS] = changed;
          wroteUrl(this);
        }
      },
    });
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
  // Blob, File and FormData
  //
  // A blob is bytes with a media type on it, so that is what this is:
  // the bytes held whole, a slice that copies, and the same promises a
  // body is read through. A form is a list of pairs and the two wire
  // formats it turns into. None of it needs the host, so none of it is
  // an op.

  const BLOB = Symbol("blob");
  const TYPE = Symbol("type");
  const FIELDS = Symbol("fields");

  /// A media type is printable ascii, lowercased, or it is nothing:
  /// what a blob does with one it cannot use is drop it rather than
  /// refuse the blob.
  function mediaType(value) {
    const type = value === undefined || value === null ? "" : String(value);
    return /^[\x20-\x7e]*$/.test(type) ? type.toLowerCase() : "";
  }

  /// One run of bytes out of the pieces something was built from, which
  /// may themselves be blobs.
  function joined(parts) {
    const pieces = [];
    let size = 0;
    for (const part of parts) {
      const bytes = part instanceof Blob ? part[BLOB] : bytesOf(part);
      pieces.push(bytes);
      size += bytes.length;
    }
    const bytes = new Uint8Array(size);
    let at = 0;
    for (const piece of pieces) {
      bytes.set(piece, at);
      at += piece.length;
    }
    return bytes;
  }

  /// Where `needle` starts in `bytes`, at or after `from`, or -1.
  ///
  /// Multipart is a byte format and a part can hold anything, so this
  /// searches bytes rather than decoding first: text that is not utf-8
  /// would not survive the round trip.
  function indexOfBytes(bytes, needle, from = 0) {
    for (let at = from; at + needle.length <= bytes.length; at += 1) {
      let same = true;
      for (let step = 0; step < needle.length; step += 1) {
        if (bytes[at + step] !== needle[step]) {
          same = false;
          break;
        }
      }
      if (same) {
        return at;
      }
    }
    return -1;
  }

  class Blob {
    constructor(parts = [], options = {}) {
      if (
        parts === null ||
        typeof parts === "string" ||
        typeof parts !== "object" ||
        typeof parts[Symbol.iterator] !== "function"
      ) {
        throw new TypeError("Blob parts must be an iterable of parts");
      }
      this[BLOB] = joined(parts);
      this[TYPE] = mediaType(options === null || options === undefined ? "" : options.type);
    }

    get size() {
      return this[BLOB].length;
    }

    get type() {
      return this[TYPE];
    }

    slice(start, end, contentType) {
      return new Blob([this[BLOB].slice(start, end)], { type: contentType });
    }

    async text() {
      return core.decode(this[BLOB]);
    }

    async bytes() {
      return this[BLOB].slice();
    }

    async arrayBuffer() {
      const bytes = this[BLOB];
      return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
    }

    stream() {
      return streamed(this[BLOB].slice());
    }
  }

  class File extends Blob {
    constructor(parts, name, options = {}) {
      super(parts, options);
      if (name === undefined) {
        throw new TypeError("File requires a name");
      }
      this.name = String(name);
      const given = options === null || options === undefined ? undefined : options.lastModified;
      this.lastModified = given === undefined ? Date.now() : Number(given);
    }
  }

  /// What a field's value is: a string, or a file. A blob that is not a
  /// file becomes one, which is the spec's rule and is why a filename
  /// can be given alongside the value.
  function fieldValue(value, filename) {
    if (value instanceof File && filename === undefined) {
      return value;
    }
    if (value instanceof Blob) {
      const named = filename === undefined ? "blob" : String(filename);
      return new File([value[BLOB]], named, { type: value.type });
    }
    return String(value);
  }

  class FormData {
    constructor() {
      this[FIELDS] = [];
    }

    append(name, value, filename) {
      this[FIELDS].push([String(name), fieldValue(value, filename)]);
    }

    delete(name) {
      const wanted = String(name);
      this[FIELDS] = this[FIELDS].filter(([held]) => held !== wanted);
    }

    get(name) {
      const found = this[FIELDS].find(([held]) => held === String(name));
      return found === undefined ? null : found[1];
    }

    getAll(name) {
      return this[FIELDS].filter(([held]) => held === String(name)).map(([, value]) => value);
    }

    has(name) {
      return this[FIELDS].some(([held]) => held === String(name));
    }

    set(name, value, filename) {
      const wanted = String(name);
      const held = fieldValue(value, filename);
      const first = this[FIELDS].findIndex(([found]) => found === wanted);
      if (first === -1) {
        this[FIELDS].push([wanted, held]);
        return;
      }
      this[FIELDS][first] = [wanted, held];
      this[FIELDS] = this[FIELDS].filter((field, at) => at <= first || field[0] !== wanted);
    }

    forEach(callback, self) {
      for (const [name, value] of this[FIELDS].slice()) {
        callback.call(self, value, name, this);
      }
    }

    *entries() {
      yield* this[FIELDS].map(([name, value]) => [name, value]);
    }

    *keys() {
      for (const [name] of this[FIELDS]) {
        yield name;
      }
    }

    *values() {
      for (const [, value] of this[FIELDS]) {
        yield value;
      }
    }

    [Symbol.iterator]() {
      return this.entries();
    }
  }

  /// A quoted name in a part's headers, with the three characters that
  /// would end the quoting written the way the spec writes them.
  function escaped(name) {
    return String(name).replace(/\r/g, "%0D").replace(/\n/g, "%0A").replace(/"/g, "%22");
  }

  /// A boundary that is not anywhere in the form it delimits.
  ///
  /// The spec's answer is a random string, and a random string that
  /// collides is a body that parses back wrong. There is no randomness
  /// in this runtime yet, so this counts: a candidate found anywhere in
  /// the form is not used and the next one is tried.
  function boundaryFor(form) {
    for (let attempt = 0; ; attempt += 1) {
      const boundary = `----zouFormBoundary${attempt}`;
      const needle = encoder.encode(boundary);
      const clash = form[FIELDS].some(([name, value]) =>
        typeof value === "string"
          ? name.includes(boundary) || value.includes(boundary)
          : name.includes(boundary) ||
            value.name.includes(boundary) ||
            indexOfBytes(value[BLOB], needle) !== -1,
      );
      if (!clash) {
        return boundary;
      }
    }
  }

  /// A form as `multipart/form-data`, with the content type naming the
  /// boundary it was written with.
  function multipart(form) {
    const boundary = boundaryFor(form);
    const pieces = [];
    for (const [name, value] of form[FIELDS]) {
      let head = `--${boundary}\r\nContent-Disposition: form-data; name="${escaped(name)}"`;
      if (typeof value !== "string") {
        head += `; filename="${escaped(value.name)}"`;
        head += `\r\nContent-Type: ${value.type === "" ? "application/octet-stream" : value.type}`;
      }
      pieces.push(encoder.encode(`${head}\r\n\r\n`));
      pieces.push(typeof value === "string" ? encoder.encode(value) : value[BLOB]);
      pieces.push(encoder.encode("\r\n"));
    }
    pieces.push(encoder.encode(`--${boundary}--\r\n`));
    return [joined(pieces), `multipart/form-data; boundary=${boundary}`];
  }

  /// The boundary a content type names, when it names one.
  function boundaryOf(contentType) {
    const parameters = String(contentType).split(";");
    if (parameters[0].trim().toLowerCase() !== "multipart/form-data") {
      return null;
    }
    for (const parameter of parameters.slice(1)) {
      const equals = parameter.indexOf("=");
      if (equals !== -1 && parameter.slice(0, equals).trim().toLowerCase() === "boundary") {
        return parameter
          .slice(equals + 1)
          .trim()
          .replace(/^"|"$/g, "");
      }
    }
    return null;
  }

  /// A multipart body read back into a form.
  ///
  /// Written out here for the reason the server writes its own out:
  /// what is needed of multipart is a delimiter, two headers per part
  /// and bytes. A part that makes no sense is dropped rather than
  /// thrown over, so one bad part does not lose the ones around it.
  function formOf(bytes, boundary) {
    const form = new FormData();
    const delimiter = encoder.encode(`--${boundary}`);
    const blank = encoder.encode("\r\n\r\n");
    let at = indexOfBytes(bytes, delimiter);
    while (at !== -1) {
      const from = at + delimiter.length;
      // The last delimiter of a body has two more dashes on it, and
      // what is after it is not a part.
      if (bytes[from] === 45 && bytes[from + 1] === 45) {
        break;
      }
      const next = indexOfBytes(bytes, delimiter, from);
      const chunk = bytes.slice(from, next === -1 ? bytes.length : next);
      at = next;
      const gap = indexOfBytes(chunk, blank);
      if (gap === -1) {
        continue;
      }
      let body = chunk.slice(gap + blank.length);
      if (body[body.length - 2] === 13 && body[body.length - 1] === 10) {
        body = body.slice(0, -2);
      }
      let name = null;
      let filename;
      let type = "";
      for (const line of core.decode(chunk.slice(0, gap)).split("\r\n")) {
        const colon = line.indexOf(":");
        if (colon === -1) {
          continue;
        }
        const field = line.slice(0, colon).trim().toLowerCase();
        const value = line.slice(colon + 1);
        if (field === "content-type") {
          type = value.trim();
        }
        if (field !== "content-disposition") {
          continue;
        }
        for (const parameter of value.split(";")) {
          const equals = parameter.indexOf("=");
          if (equals === -1) {
            continue;
          }
          const key = parameter.slice(0, equals).trim().toLowerCase();
          const given = parameter
            .slice(equals + 1)
            .trim()
            .replace(/^"|"$/g, "");
          if (key === "name") {
            name = given;
          }
          if (key === "filename") {
            filename = given;
          }
        }
      }
      if (name !== null) {
        form.append(
          name,
          filename === undefined ? core.decode(body) : new File([body], filename, { type }),
        );
      }
    }
    return form;
  }

  // ---------------------------------------------------------------
  // ---------------------------------------------------------------
  // Streams
  //
  // A queue, the readers waiting on it, and the source's three
  // functions. That is the whole of what a readable stream is, and the
  // parts of the spec that are not here are the parts that exist for a
  // browser: there is no byte stream and so no BYOB reader, no
  // `WritableStream` and so no `pipeTo`, and the queueing strategy is a
  // count of chunks rather than a size in bytes.
  //
  // What a function reaches for is here: a source it wrote itself, a
  // reader, `for await`, `tee` and `cancel`.

  const STREAM = Symbol("stream");

  class ReadableStream {
    constructor(source = {}, strategy = {}) {
      source = source ?? {};
      if (source.type !== undefined && source.type !== null) {
        throw new TypeError(`a ${source.type} stream is not supported yet`);
      }
      const held = {
        source,
        queue: [],
        waiting: [],
        state: "readable",
        stored: undefined,
        locked: false,
        pulling: false,
        want: Number(strategy.highWaterMark ?? 1),
        started: null,
      };
      this[STREAM] = held;
      const controller = {
        enqueue(chunk) {
          arrives(held, chunk);
        },
        close() {
          ends(held);
        },
        error(why) {
          fails(held, why);
        },
        get desiredSize() {
          return held.state === "readable" ? held.want - held.queue.length : null;
        },
      };
      held.controller = controller;
      // `start` may be a promise, and nothing is pulled until it has
      // settled, which is what lets a source do its setup before it is
      // asked for anything.
      held.started = (async () => {
        if (typeof source.start === "function") {
          await source.start(controller);
        }
      })().catch((thrown) => fails(held, thrown));
      pulls(held);
    }

    get locked() {
      return this[STREAM].locked;
    }

    getReader(options = {}) {
      if (options !== null && options !== undefined && options.mode !== undefined) {
        throw new TypeError(`a ${options.mode} reader is not supported yet`);
      }
      return new ReadableStreamDefaultReader(this);
    }

    cancel(reason) {
      if (this[STREAM].locked) {
        return Promise.reject(new TypeError("the stream is locked to a reader"));
      }
      return gives(this[STREAM], reason);
    }

    /// Two streams that both see every chunk, which is the only way to
    /// read a body twice.
    tee() {
      const reader = this.getReader();
      const sides = [];
      const both = (each) => {
        for (const side of sides) {
          each(side);
        }
      };
      let reading = false;
      const pull = async () => {
        if (reading) {
          return;
        }
        reading = true;
        try {
          const { value, done } = await reader.read();
          if (done) {
            both((side) => ends(side));
          } else {
            both((side) => arrives(side, value));
          }
        } catch (thrown) {
          both((side) => fails(side, thrown));
        } finally {
          reading = false;
        }
      };
      const made = [
        new ReadableStream({ pull }),
        new ReadableStream({ pull }),
      ];
      for (const side of made) {
        sides.push(side[STREAM]);
      }
      return made;
    }

    /// Everything this gives out, written to somebody else's sink,
    /// one chunk at a time so the writer's backpressure is felt here.
    async pipeTo(destination, options = {}) {
      options = options ?? {};
      const reader = this.getReader();
      const writer = destination.getWriter();
      try {
        for (;;) {
          const { value, done } = await reader.read();
          if (done) {
            break;
          }
          await writer.ready;
          await writer.write(value);
        }
        if (!options.preventClose) {
          await writer.close();
        }
      } catch (thrown) {
        if (!options.preventAbort) {
          await writer.abort(thrown).catch(() => {});
        }
        if (!options.preventCancel) {
          await reader.cancel(thrown).catch(() => {});
        }
        throw thrown;
      } finally {
        reader.releaseLock();
        writer.releaseLock();
      }
    }

    /// The same, through a pair, which is how a transform is used:
    /// `stream.pipeThrough(new TransformStream(...))`. The piping is
    /// left running and the readable side is the answer.
    pipeThrough(pair, options = {}) {
      if (pair === null || pair === undefined) {
        throw new TypeError("pipeThrough requires a writable and a readable");
      }
      const { writable, readable } = pair;
      if (writable === undefined || readable === undefined) {
        throw new TypeError("pipeThrough requires a writable and a readable");
      }
      // Nobody is obliged to look at how the piping went, and the
      // failure is on the readable side to report either way.
      this.pipeTo(writable, options).catch(() => {});
      return readable;
    }

    async *[Symbol.asyncIterator]() {
      const reader = this.getReader();
      try {
        for (;;) {
          const { value, done } = await reader.read();
          if (done) {
            return;
          }
          yield value;
        }
      } finally {
        reader.releaseLock();
      }
    }

    values() {
      return this[Symbol.asyncIterator]();
    }

    /// Deno has this and libraries reach for it, and everything it
    /// needs is already here.
    static from(source) {
      const iterator =
        typeof source[Symbol.asyncIterator] === "function"
          ? source[Symbol.asyncIterator]()
          : source[Symbol.iterator]();
      return new ReadableStream({
        async pull(controller) {
          const { value, done } = await iterator.next();
          if (done) {
            controller.close();
          } else {
            controller.enqueue(value);
          }
        },
        async cancel(reason) {
          if (typeof iterator.return === "function") {
            await iterator.return(reason);
          }
        },
      });
    }
  }

  class ReadableStreamDefaultReader {
    constructor(stream) {
      const held = stream[STREAM];
      if (held.locked) {
        throw new TypeError("the stream is locked to a reader");
      }
      held.locked = true;
      this[STREAM] = held;
      this.closed = new Promise((done, broken) => {
        held.told = { done, broken };
      });
      // Nobody is obliged to look at `closed`, and a promise nobody
      // looks at that rejects is an unhandled rejection.
      this.closed.catch(() => {});
    }

    read() {
      const held = this[STREAM];
      if (held === null) {
        return Promise.reject(new TypeError("the reader has been released"));
      }
      return reads(held);
    }

    cancel(reason) {
      const held = this[STREAM];
      if (held === null) {
        return Promise.reject(new TypeError("the reader has been released"));
      }
      return gives(held, reason);
    }

    releaseLock() {
      const held = this[STREAM];
      if (held === null) {
        return;
      }
      held.locked = false;
      this[STREAM] = null;
    }
  }

  function arrives(held, chunk) {
    if (held.state !== "readable") {
      return;
    }
    const waiting = held.waiting.shift();
    if (waiting === undefined) {
      held.queue.push(chunk);
    } else {
      waiting.done({ value: chunk, done: false });
    }
  }

  function ends(held) {
    if (held.state !== "readable") {
      return;
    }
    held.state = "closed";
    for (const waiting of held.waiting.splice(0)) {
      waiting.done({ value: undefined, done: true });
    }
    held.told?.done(undefined);
  }

  function fails(held, why) {
    if (held.state !== "readable") {
      return;
    }
    held.state = "errored";
    held.stored = why;
    held.queue.length = 0;
    for (const waiting of held.waiting.splice(0)) {
      waiting.broken(why);
    }
    held.told?.broken(why);
  }

  async function gives(held, reason) {
    if (held.state === "readable") {
      held.queue.length = 0;
      ends(held);
      if (typeof held.source.cancel === "function") {
        await held.source.cancel(reason);
      }
    }
  }

  function reads(held) {
    if (held.queue.length > 0) {
      const value = held.queue.shift();
      pulls(held);
      return Promise.resolve({ value, done: false });
    }
    if (held.state === "closed") {
      return Promise.resolve({ value: undefined, done: true });
    }
    if (held.state === "errored") {
      return Promise.reject(held.stored);
    }
    const waited = new Promise((done, broken) => {
      held.waiting.push({ done, broken });
    });
    pulls(held);
    return waited;
  }

  /// Ask the source for more, once at a time, until it has given us as
  /// much as was asked for or somebody is still waiting.
  async function pulls(held) {
    if (held.pulling || typeof held.source.pull !== "function") {
      return;
    }
    held.pulling = true;
    try {
      await held.started;
      while (
        held.state === "readable" &&
        (held.waiting.length > 0 || held.queue.length < held.want)
      ) {
        const before = held.queue.length + held.waiting.length;
        await held.source.pull(held.controller);
        if (held.queue.length + held.waiting.length === before && held.waiting.length === 0) {
          // A pull that gave us nothing and left nobody waiting is a
          // source that will speak when it is ready.
          break;
        }
      }
    } catch (thrown) {
      fails(held, thrown);
    } finally {
      held.pulling = false;
    }
  }

  /// Every chunk of a stream, as one run of bytes, which is what a body
  /// is once somebody has asked for all of it.
  async function collected(stream) {
    const held = [];
    let length = 0;
    for await (const chunk of stream) {
      if (!(chunk instanceof Uint8Array) && !ArrayBuffer.isView(chunk) && !(chunk instanceof ArrayBuffer)) {
        throw new TypeError("a body stream may only give out bytes");
      }
      const bytes = bytesOf(chunk);
      held.push(bytes);
      length += bytes.length;
    }
    const all = new Uint8Array(length);
    let at = 0;
    for (const bytes of held) {
      all.set(bytes, at);
      at += bytes.length;
    }
    return all;
  }

  // ---------------------------------------------------------------
  // The writable half
  //
  // A sink, one write at a time, and the promise a writer waits on.
  // The queueing strategy is a count of chunks the same way the
  // readable half's is, so `desiredSize` is one minus what is in
  // flight rather than a size in bytes.
  //
  // This is here because a stream is rarely written without one:
  // `TransformStream` below is a writable side and a readable side
  // tied together, `pipeTo` needs somewhere to pipe to, and the two
  // Supabase examples that would not load at all, `oak` and
  // `connect-supabase`, both reference `TransformStream` while their
  // module is being evaluated.

  const SINK = Symbol("sink");

  class WritableStream {
    constructor(sink = {}, strategy = {}) {
      sink = sink ?? {};
      if (sink.type !== undefined && sink.type !== null) {
        throw new TypeError(`a ${sink.type} sink is not supported yet`);
      }
      const held = {
        sink,
        state: "writable",
        stored: undefined,
        locked: false,
        inflight: 0,
        want: Number(strategy.highWaterMark ?? 1),
        // Writes go through in the order they were made, which is what
        // makes a stream a stream rather than a pile of promises.
        last: Promise.resolve(),
        told: null,
      };
      held.closed = new Promise((done, broken) => {
        held.told = { done, broken };
      });
      held.closed.catch(() => {});
      this[SINK] = held;
      const controller = {
        error(why) {
          stops(held, why);
        },
        signal: new AbortController().signal,
      };
      held.controller = controller;
      held.started = (async () => {
        if (typeof sink.start === "function") {
          await sink.start(controller);
        }
      })().catch((thrown) => stops(held, thrown));
    }

    get locked() {
      return this[SINK].locked;
    }

    getWriter() {
      return new WritableStreamDefaultWriter(this);
    }

    abort(reason) {
      if (this[SINK].locked) {
        return Promise.reject(new TypeError("the stream is locked to a writer"));
      }
      return aborts(this[SINK], reason);
    }

    close() {
      if (this[SINK].locked) {
        return Promise.reject(new TypeError("the stream is locked to a writer"));
      }
      return closes(this[SINK]);
    }
  }

  class WritableStreamDefaultWriter {
    constructor(stream) {
      const held = stream[SINK];
      if (held.locked) {
        throw new TypeError("the stream is locked to a writer");
      }
      held.locked = true;
      this[SINK] = held;
      this.closed = held.closed;
    }

    /// The backpressure, such as it is: the last write, so a writer
    /// that awaits this is a writer that is not ahead of the sink.
    get ready() {
      const held = this[SINK];
      if (held === null) {
        return Promise.reject(new TypeError("the writer has been released"));
      }
      return held.last.then(
        () => undefined,
        () => undefined,
      );
    }

    get desiredSize() {
      const held = this[SINK];
      if (held === null) {
        throw new TypeError("the writer has been released");
      }
      return held.state === "writable" ? held.want - held.inflight : null;
    }

    write(chunk) {
      const held = this[SINK];
      if (held === null) {
        return Promise.reject(new TypeError("the writer has been released"));
      }
      return writes(held, chunk);
    }

    close() {
      const held = this[SINK];
      if (held === null) {
        return Promise.reject(new TypeError("the writer has been released"));
      }
      return closes(held);
    }

    abort(reason) {
      const held = this[SINK];
      if (held === null) {
        return Promise.reject(new TypeError("the writer has been released"));
      }
      return aborts(held, reason);
    }

    releaseLock() {
      const held = this[SINK];
      if (held === null) {
        return;
      }
      held.locked = false;
      this[SINK] = null;
    }
  }

  function stops(held, why) {
    if (held.state !== "writable" && held.state !== "closing") {
      return;
    }
    held.state = "errored";
    held.stored = why;
    held.told.broken(why);
  }

  function writes(held, chunk) {
    if (held.state === "errored") {
      return Promise.reject(held.stored);
    }
    if (held.state !== "writable") {
      return Promise.reject(new TypeError("the stream is closed"));
    }
    held.inflight += 1;
    const written = held.last.then(async () => {
      await held.started;
      if (held.state === "errored") {
        throw held.stored;
      }
      if (typeof held.sink.write === "function") {
        await held.sink.write(chunk, held.controller);
      }
    });
    // The queue is this promise chain, so a write that failed is not
    // allowed to stop the ones behind it from being attempted, and the
    // failure is still the caller's to see.
    held.last = written.then(
      () => {
        held.inflight -= 1;
      },
      (thrown) => {
        held.inflight -= 1;
        stops(held, thrown);
      },
    );
    return written;
  }

  async function closes(held) {
    if (held.state === "errored") {
      throw held.stored;
    }
    if (held.state !== "writable") {
      return;
    }
    const after = held.last;
    held.state = "closing";
    await after;
    await held.started;
    if (held.state === "errored") {
      throw held.stored;
    }
    if (typeof held.sink.close === "function") {
      await held.sink.close();
    }
    held.state = "closed";
    held.told.done(undefined);
  }

  async function aborts(held, reason) {
    if (held.state === "closed" || held.state === "errored") {
      return;
    }
    const sink = held.sink;
    stops(held, reason);
    if (typeof sink.abort === "function") {
      await sink.abort(reason);
    }
  }

  /// A writable side and a readable side, with whatever the caller
  /// wrote in between. A transformer that says nothing passes chunks
  /// through, which is what `new TransformStream()` on its own is for.
  class TransformStream {
    constructor(transformer = {}, writableStrategy = {}, readableStrategy = {}) {
      transformer = transformer ?? {};
      if (transformer.readableType !== undefined || transformer.writableType !== undefined) {
        throw new TypeError("a typed transform stream is not supported yet");
      }
      let side = null;
      const readable = new ReadableStream(
        {
          start(controller) {
            side = controller;
          },
        },
        readableStrategy,
      );
      const controller = {
        enqueue(chunk) {
          side.enqueue(chunk);
        },
        terminate() {
          side.close();
        },
        error(why) {
          side.error(why);
        },
        get desiredSize() {
          return side.desiredSize;
        },
      };
      const writable = new WritableStream(
        {
          async start() {
            if (typeof transformer.start === "function") {
              await transformer.start(controller);
            }
          },
          async write(chunk) {
            if (typeof transformer.transform === "function") {
              await transformer.transform(chunk, controller);
            } else {
              controller.enqueue(chunk);
            }
          },
          async close() {
            if (typeof transformer.flush === "function") {
              await transformer.flush(controller);
            }
            side.close();
          },
          async abort(reason) {
            side.error(reason);
          },
        },
        writableStrategy,
      );
      this.readable = readable;
      this.writable = writable;
    }
  }

  /// One run of bytes as a stream, which is what `.body` is for a body
  /// that was never a stream to begin with.
  function streamed(bytes, taken) {
    return new ReadableStream({
      start(controller) {
        taken?.();
        if (bytes.length > 0) {
          controller.enqueue(bytes);
        }
        controller.close();
      },
    });
  }

  // ---------------------------------------------------------------
  // Bodies

  const BODY = Symbol("body");
  const USED = Symbol("bodyUsed");
  const SOURCE = Symbol("bodySource");

  /// The bytes of a body, whether they were bytes all along or are
  /// still arriving.
  async function readBody(target) {
    if (target[USED]) {
      throw new TypeError("Body already consumed.");
    }
    target[USED] = true;
    if (target[SOURCE] !== null && target[SOURCE] !== undefined) {
      const stream = target[SOURCE];
      target[SOURCE] = null;
      target[BODY] = await collected(stream);
    }
    return target[BODY] ?? new Uint8Array(0);
  }

  /// What goes out on the wire, which is not a read: a response the
  /// host is sending is not a body the handler consumed.
  async function sending(target) {
    if (target[SOURCE] !== null && target[SOURCE] !== undefined) {
      const stream = target[SOURCE];
      target[SOURCE] = null;
      target[BODY] = await collected(stream);
    }
    return target[BODY] ?? new Uint8Array(0);
  }

  const bodyMethods = {
    async arrayBuffer() {
      const bytes = await readBody(this);
      return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
    },
    async bytes() {
      return (await readBody(this)).slice();
    },
    async text() {
      return core.decode(await readBody(this));
    },
    async json() {
      return JSON.parse(core.decode(await readBody(this)));
    },
    /// The body's own content type is the blob's, because that is the
    /// only place the type of some bytes is written down here.
    async blob() {
      return new Blob([await readBody(this)], { type: this.headers.get("content-type") ?? "" });
    },
    async formData() {
      const type = this.headers.get("content-type") ?? "";
      const boundary = boundaryOf(type);
      const bytes = await readBody(this);
      if (boundary !== null) {
        return formOf(bytes, boundary);
      }
      if (type.split(";")[0].trim().toLowerCase() === "application/x-www-form-urlencoded") {
        const form = new FormData();
        for (const [name, value] of pairsOf(core.decode(bytes))) {
          form.append(name, value);
        }
        return form;
      }
      throw new TypeError("Body can not be decoded as form data");
    },
    get bodyUsed() {
      return this[USED];
    },
    /// A stream either way: the one the body was made of, or one over
    /// the bytes it was made of, made the first time somebody asks.
    get body() {
      if (this[SOURCE] !== null && this[SOURCE] !== undefined) {
        return this[SOURCE];
      }
      if (this[BODY] === null || this[BODY] === undefined) {
        return null;
      }
      const made = streamed(this[BODY], () => {
        this[USED] = true;
      });
      this[SOURCE] = made;
      return made;
    },
  };

  /// The bytes of a body, the stream it is still arriving on, and the
  /// content type it implies when the caller did not name one. A body
  /// that is null is not a body: `res.body` is null and reading it is
  /// an empty string, which are different things from an empty body.
  function intoBody(body) {
    if (body === undefined || body === null) {
      return [null, null, null];
    }
    if (typeof body === "string") {
      return [encoder.encode(body), "text/plain;charset=UTF-8", null];
    }
    if (body instanceof Uint8Array || body instanceof ArrayBuffer || ArrayBuffer.isView(body)) {
      return [bytesOf(body), null, null];
    }
    if (body instanceof Blob) {
      return [body[BLOB].slice(), body.type === "" ? null : body.type, null];
    }
    if (body instanceof FormData) {
      const [bytes, type] = multipart(body);
      return [bytes, type, null];
    }
    if (body instanceof URLSearchParams) {
      return [encoder.encode(body.toString()), "application/x-www-form-urlencoded;charset=UTF-8", null];
    }
    if (body instanceof ReadableStream) {
      return [null, null, body];
    }
    return [encoder.encode(String(body)), "text/plain;charset=UTF-8", null];
  }

  // ---------------------------------------------------------------
  // Request and Response

  class Request {
    constructor(input, init = {}) {
      if (input instanceof Request) {
        this.url = input.url;
        this.method = init.method ? String(init.method).toUpperCase() : input.method;
        this.headers = new Headers(init.headers ?? input.headers);
        if (init.body === undefined) {
          this[BODY] = input[BODY];
          this[SOURCE] = input[SOURCE];
        } else {
          const [bytes, , stream] = intoBody(init.body);
          this[BODY] = bytes;
          this[SOURCE] = stream;
        }
      } else {
        // A request's url is a url, so it is parsed here and not left
        // as whatever string it arrived as: `new Request("/one")` has
        // nothing to be relative to and is an error, the same as Deno.
        this.url = new URL(input).href;
        this.method = init.method ? String(init.method).toUpperCase() : "GET";
        this.headers = new Headers(init.headers);
        const [bytes, type, stream] = intoBody(init.body);
        this[BODY] = bytes;
        this[SOURCE] = stream;
        if (type !== null && !this.headers.has("content-type")) {
          this.headers.set("content-type", type);
        }
      }
      this[USED] = false;
    }

    clone() {
      const copy = new Request(this.url, { method: this.method, headers: this.headers });
      // A stream cannot be in two places, so cloning one is teeing it,
      // which is what the spec says to do and is why `tee` exists.
      if (this[SOURCE] !== null && this[SOURCE] !== undefined) {
        const [mine, theirs] = this[SOURCE].tee();
        this[SOURCE] = mine;
        copy[SOURCE] = theirs;
      } else {
        copy[BODY] = this[BODY];
      }
      return copy;
    }
  }
  Object.defineProperties(Request.prototype, Object.getOwnPropertyDescriptors(bodyMethods));

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
      const [bytes, type, stream] = intoBody(body);
      this[BODY] = bytes;
      this[SOURCE] = stream;
      this[USED] = false;
      if (type !== null && !this.headers.has("content-type")) {
        this.headers.set("content-type", type);
      }
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
      if (this[SOURCE] !== null && this[SOURCE] !== undefined) {
        const [mine, theirs] = this[SOURCE].tee();
        this[SOURCE] = mine;
        copy[SOURCE] = theirs;
      } else {
        copy[BODY] = this[BODY];
      }
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
  Object.defineProperties(Response.prototype, Object.getOwnPropertyDescriptors(bodyMethods));

  // ---------------------------------------------------------------
  // fetch

  const FETCHABLE = ["http:", "https:"];

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
    res[SOURCE] = null;
    res[USED] = false;
    Object.defineProperty(res, "redirected", { value: answer.redirected });
    return res;
  }

  async function fetch(input, init) {
    const request = input instanceof Request && init === undefined ? input : new Request(input, init ?? {});
    // The Request constructor already refused what is not a url, so
    // what is left to say is which schemes are served.
    const scheme = new URL(request.url).protocol;
    if (!FETCHABLE.includes(scheme)) {
      throw new TypeError(`fetch does not serve the ${scheme.slice(0, -1)} scheme yet`);
    }
    // Reading the body is what sending it is, and a request whose body
    // was already read is a request with nothing left to send.
    const body = await readBody(request);
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
  // crypto
  //
  // Randomness is the operating system's and a hash is the host's, so
  // both are ops. What is here is the shape web crypto has: the names
  // normalised before they cross, a key object that holds bytes and
  // the hash it was made for, and promises around calls that are not
  // actually asynchronous.

  const SECRET = Symbol("secret");

  const HASHES = ["SHA-1", "SHA-256", "SHA-384", "SHA-512"];

  /// The bytes of a buffer source, and a refusal for anything that is
  /// not one. `bytesOf` would stringify what it does not know, and a
  /// hash of the word `[object Object]` is worse than an error.
  function sourceOf(value, called) {
    if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
      return bytesOf(value);
    }
    throw new TypeError(`${called} must be a BufferSource`);
  }

  /// A hash named the way the spec names it, out of a string or an
  /// object with a name on it, and a `TypeError` for anything else.
  function hashNamed(algorithm) {
    const given =
      algorithm !== null && typeof algorithm === "object"
        ? String(algorithm.name ?? "")
        : String(algorithm);
    const found = HASHES.find((name) => name.toLowerCase() === given.toLowerCase());
    if (found === undefined) {
      throw new TypeError(`Unrecognized algorithm name: ${given}`);
    }
    return found;
  }

  /// The hash an HMAC operation is under, which is the key's and not
  /// the algorithm's: `sign("HMAC", key, data)` names no hash at all.
  function hmacHash(algorithm, key) {
    const named =
      algorithm !== null && typeof algorithm === "object"
        ? String(algorithm.name ?? "")
        : String(algorithm);
    if (named.toLowerCase() !== "hmac") {
      throw new TypeError(`${named} is not supported yet, only HMAC is`);
    }
    if (!(key instanceof CryptoKey)) {
      throw new TypeError("a key is required");
    }
    return key.algorithm.hash.name;
  }

  class CryptoKey {
    constructor(bytes, hash, extractable, usages) {
      this[SECRET] = bytes;
      this.type = "secret";
      this.extractable = Boolean(extractable);
      this.usages = usages;
      this.algorithm = { name: "HMAC", hash: { name: hash }, length: bytes.length * 8 };
    }
  }

  function refuse(name) {
    return () => {
      throw new TypeError(`crypto.subtle.${name} is not supported yet`);
    };
  }

  const subtle = {
    async digest(algorithm, data) {
      const digested = ops.op_zou_digest(hashNamed(algorithm), sourceOf(data, "data"));
      return digested.buffer;
    },

    /// Raw HMAC keys and nothing else, because that is what a function
    /// verifying a webhook or signing its own token needs and the rest
    /// wants key formats this has no parser for.
    async importKey(format, keyData, algorithm, extractable, usages) {
      if (String(format) !== "raw") {
        throw new TypeError(`the ${format} key format is not supported yet, only raw is`);
      }
      const named =
        algorithm !== null && typeof algorithm === "object"
          ? String(algorithm.name ?? "")
          : String(algorithm);
      if (named.toLowerCase() !== "hmac") {
        throw new TypeError(`${named} keys are not supported yet, only HMAC is`);
      }
      const hash = hashNamed(algorithm.hash);
      return new CryptoKey(
        sourceOf(keyData, "keyData"),
        hash,
        extractable,
        Array.from(usages ?? []).map(String),
      );
    },

    async sign(algorithm, key, data) {
      const hash = hmacHash(algorithm, key);
      const signature = ops.op_zou_sign(hash, key[SECRET], sourceOf(data, "data"));
      return signature.buffer;
    },

    /// The comparison is the host's, because a comparison here would
    /// stop at the first byte that differs and how long a wrong answer
    /// took is how a signature is guessed.
    async verify(algorithm, key, signature, data) {
      const hash = hmacHash(algorithm, key);
      return ops.op_zou_verify(
        hash,
        key[SECRET],
        sourceOf(data, "data"),
        sourceOf(signature, "signature"),
      );
    },

    encrypt: refuse("encrypt"),
    decrypt: refuse("decrypt"),
    deriveBits: refuse("deriveBits"),
    deriveKey: refuse("deriveKey"),
    exportKey: refuse("exportKey"),
    generateKey: refuse("generateKey"),
    unwrapKey: refuse("unwrapKey"),
    wrapKey: refuse("wrapKey"),
  };

  // The spec's own ceiling on one call, so a function that asks for a
  // gigabyte of randomness is told no here rather than being handed it.
  const RANDOM_LIMIT = 65536;

  const crypto = {
    subtle,

    getRandomValues(into) {
      if (!ArrayBuffer.isView(into) || into instanceof DataView) {
        throw new TypeError("The provided value is not of type '(ArrayBufferView or ArrayBuffer)'");
      }
      if (into instanceof Float32Array || into instanceof Float64Array) {
        throw new TypeError("The provided ArrayBufferView is not an integer array type");
      }
      if (into.byteLength > RANDOM_LIMIT) {
        throw new TypeError(
          `The ArrayBufferView's byte length (${into.byteLength}) exceeds the number of bytes of entropy available via this API (${RANDOM_LIMIT})`,
        );
      }
      ops.op_zou_random(new Uint8Array(into.buffer, into.byteOffset, into.byteLength));
      return into;
    },

    randomUUID() {
      const bytes = new Uint8Array(16);
      ops.op_zou_random(bytes);
      // Version 4 and the variant, which are the two fields of a uuid
      // that are not random.
      bytes[6] = (bytes[6] & 0x0f) | 0x40;
      bytes[8] = (bytes[8] & 0x3f) | 0x80;
      const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
      return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
    },
  };

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
  // Timers
  //
  // The sleeping is an op and the bookkeeping is here: which timers are
  // still wanted, and the callback each one is for. A cleared timer is
  // taken out of the set and cancelled at the host, so a timer set for
  // an hour and cleared is not an hour of a future being held.

  const timers = new Set();
  let nextTimer = 1;

  function after(callback, delay, args, repeating) {
    if (typeof callback !== "function") {
      // Deno takes a string here and evaluates it. That is `eval` with
      // a longer name, and refusing it is a difference in the direction
      // of no.
      throw new TypeError("a timer needs a function, and a string of code is not one here");
    }
    const id = nextTimer;
    nextTimer += 1;
    timers.add(id);
    const wait = Number(delay);
    (async () => {
      while (timers.has(id)) {
        const fired = await ops.op_zou_sleep(id, wait);
        if (!fired || !timers.has(id)) {
          break;
        }
        if (!repeating) {
          timers.delete(id);
        }
        try {
          callback(...args);
        } catch (thrown) {
          // A handler cannot catch this: by the time the timer fires,
          // whatever set it has returned. Deno's answer is to end the
          // process, which here would be to lose an answer that is
          // already written, so this says so and the call goes on.
          console.error(thrown);
        }
      }
      timers.delete(id);
    })();
    return id;
  }

  function setTimeout(callback, delay = 0, ...args) {
    return after(callback, delay, args, false);
  }

  function setInterval(callback, delay = 0, ...args) {
    return after(callback, delay, args, true);
  }

  function clearTimer(id) {
    const held = Number(id);
    if (timers.delete(held)) {
      ops.op_zou_clear(held);
    }
  }

  function queueMicrotask(callback) {
    if (typeof callback !== "function") {
      throw new TypeError("queueMicrotask requires a function");
    }
    Promise.resolve().then(() => {
      try {
        callback();
      } catch (thrown) {
        console.error(thrown);
      }
    });
  }

  // ---------------------------------------------------------------
  // performance
  //
  // The clock a library reaches for when it wants a duration rather
  // than a date, and enough of a reason on its own for two of the
  // examples not to load: `@sentry/deno` reads it while the sdk is
  // being initialised, at the top of the module, so the function is
  // gone before it has served anything.
  //
  // `timeOrigin` is when this isolate started, in wall clock
  // milliseconds, and `now` counts from there on a monotonic clock. An
  // isolate that is kept and called again keeps counting, which is what
  // the number means upstream too, where a worker is what holds it.
  //
  // The entry buffer is not here: `mark`, `measure` and the
  // `getEntries` family are absent rather than faked, because a
  // library that finds them expects to be able to read back what it
  // recorded, and an empty list is a worse answer than no method at
  // all. `docs/functions.md` says so.

  const started = Date.now();

  // ---------------------------------------------------------------
  // navigator
  //
  // Four properties, and the reason to have them is that a library
  // reads one of them to work out what it is running on. `@sentry/deno`
  // reads `navigator.userAgent` while the sdk is being initialised, so
  // a function that imports it does not load without this.
  //
  // The shape and the values are upstream's, measured on a real
  // `supabase start` rather than guessed. There, this is
  // `hardwareConcurrency,userAgent,language,languages`, the user agent
  // is `Deno/2.1.4 (variant; SupabaseEdgeRuntime/1.74.2)` and the core
  // count is one whatever the host has, which is the honest answer for
  // a runtime where a function gets one thread. This says the same in
  // its own name.

  const navigator = {
    get hardwareConcurrency() {
      return 1;
    },
    get userAgent() {
      return ops.op_zou_agent();
    },
    get language() {
      return "en";
    },
    get languages() {
      return ["en"];
    },
  };

  const performance = {
    get timeOrigin() {
      return started;
    },
    now() {
      return ops.op_zou_now();
    },
    toJSON() {
      return { timeOrigin: started };
    },
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

  // ---------------------------------------------------------------
  // Deno.listen and Deno.serveHttp
  //
  // The older way of serving, which half the examples still use:
  //
  // ```ts
  // import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
  // serve(handler)
  // ```
  //
  // That `serve` is a loop over a socket. It listens, accepts a
  // connection, upgrades it, pulls requests off it one at a time and
  // answers each with `respondWith`. There is no socket here, because
  // the host holds the only one, so what is here is that shape with
  // one call in it rather than a network under it. The reference has
  // real versions of both, measured: `Deno.listen`, `Deno.serveHttp`
  // and `Deno.upgradeWebSocket` are functions on its `Deno` and
  // `Deno.upgradeHttp` is not.
  //
  // A pooled isolate serves its second call through the loop the first
  // one left running, which is why the request goes to whoever is
  // waiting rather than to a fresh connection each time.

  const accepting = {
    /// Whether a module asked for a socket at all, which is what makes
    /// this the entry point rather than `Deno.serve`.
    listening: false,
    /// Waiting `accept()` calls, and the waiting `nextRequest()` calls
    /// of the connection an accept handed out.
    accepts: [],
    nexts: [],
    /// Calls nobody has picked up yet, which is at most one.
    waiting: [],
    connected: false,
  };

  const CONN = {
    rid: 0,
    localAddr: { transport: "tcp", hostname: "0.0.0.0", port: 9000 },
    remoteAddr: { transport: "tcp", hostname: "0.0.0.0", port: 0 },
    close() {},
    readable: null,
    writable: null,
  };

  function listen(options = {}) {
    accepting.listening = true;
    return {
      rid: 0,
      addr: {
        transport: options.transport ?? "tcp",
        hostname: options.hostname ?? "0.0.0.0",
        port: options.port ?? 9000,
      },
      accept() {
        if (!accepting.connected && accepting.waiting.length > 0) {
          accepting.connected = true;
          return Promise.resolve(CONN);
        }
        return new Promise((resolve) => accepting.accepts.push(resolve));
      },
      close() {},
      ref() {},
      unref() {},
      [Symbol.asyncIterator]() {
        const listener = this;
        return {
          async next() {
            return { value: await listener.accept(), done: false };
          },
        };
      },
    };
  }

  function serveHttp(conn) {
    if (conn !== CONN) {
      throw new BadResource("this connection is not one this runtime handed out");
    }
    return {
      rid: 0,
      nextRequest() {
        const held = accepting.waiting.shift();
        if (held !== undefined) {
          return Promise.resolve(held);
        }
        return new Promise((resolve) => accepting.nexts.push(resolve));
      },
      close() {},
      [Symbol.asyncIterator]() {
        const http = this;
        return {
          async next() {
            const event = await http.nextRequest();
            return event === null ? { value: undefined, done: true } : { value: event, done: false };
          },
        };
      },
    };
  }

  /// Hand this call to whatever part of that loop is waiting for one,
  /// and resolve with the response the loop answers it with.
  function accepted(request) {
    return new Promise((resolve, reject) => {
      const event = {
        request,
        respondWith(answer) {
          return Promise.resolve(answer).then(
            (given) => {
              resolve(given);
              return undefined;
            },
            (thrown) => {
              reject(thrown);
              throw thrown;
            },
          );
        },
      };
      const next = accepting.nexts.shift();
      if (next !== undefined) {
        next(event);
        return;
      }
      accepting.waiting.push(event);
      const accept = accepting.accepts.shift();
      if (accept !== undefined) {
        accepting.connected = true;
        accept(CONN);
      }
    });
  }

  // ---------------------------------------------------------------
  // Events
  //
  // Enough of an event to be the argument a websocket handler is
  // written against. There is no capture, no bubbling and no
  // propagation to stop, because there is no tree here for any of that
  // to happen in: one object dispatches to its own listeners.

  class Event {
    constructor(type, init = {}) {
      this.type = String(type);
      this.target = null;
      this.currentTarget = null;
      this.cancelable = Boolean(init.cancelable);
      this.bubbles = Boolean(init.bubbles);
      this.defaultPrevented = false;
      this.eventPhase = 0;
      this.timeStamp = 0;
    }
    preventDefault() {
      if (this.cancelable) {
        this.defaultPrevented = true;
      }
    }
    stopPropagation() {}
    stopImmediatePropagation() {
      // No tree, so nothing to stop propagating to, but the listeners
      // after this one on the same target are still listeners this is
      // supposed to stop.
      this[STOPPED] = true;
    }
  }

  class MessageEvent extends Event {
    constructor(type, init = {}) {
      super(type, init);
      this.data = init.data ?? null;
      this.origin = init.origin ?? "";
      this.lastEventId = "";
      this.source = null;
      this.ports = [];
    }
  }

  class CloseEvent extends Event {
    constructor(type, init = {}) {
      super(type, init);
      this.code = init.code ?? 0;
      this.reason = init.reason ?? "";
      this.wasClean = Boolean(init.wasClean);
    }
  }

  class ErrorEvent extends Event {
    constructor(type, init = {}) {
      super(type, init);
      this.message = init.message ?? "";
      this.filename = "";
      this.lineno = 0;
      this.colno = 0;
      this.error = init.error ?? null;
    }
  }

  /// An event with something of the caller's on it, which is how a
  /// library that emits its own events hands anything over.
  class CustomEvent extends Event {
    constructor(type, init = {}) {
      super(type, init);
      this.detail = init.detail ?? null;
    }
  }

  const LISTENERS = Symbol("listeners");
  const STOPPED = Symbol("stopped");

  function listens(target) {
    target[LISTENERS] = new Map();
  }

  function fires(target, event) {
    event.target = target;
    event.currentTarget = target;
    event[STOPPED] = false;
    const named = `on${event.type}`;
    if (typeof target[named] === "function") {
      try {
        target[named].call(target, event);
      } catch (thrown) {
        console.error(thrown);
      }
    }
    // A copy, because a listener is allowed to add or remove one while
    // it is running and the ones being run are the ones there were.
    for (const held of [...(target[LISTENERS].get(event.type) ?? [])]) {
      if (event[STOPPED]) {
        break;
      }
      if (held.once) {
        unlistened.call(target, event.type, held.listener);
      }
      try {
        if (typeof held.listener === "function") {
          held.listener.call(target, event);
        } else {
          held.listener.handleEvent(event);
        }
      } catch (thrown) {
        // One listener that throws is not the rest of them, and there
        // is nobody above this to catch it either way.
        console.error(thrown);
      }
    }
    return !event.defaultPrevented;
  }

  function listened(type, listener, options = {}) {
    if (listener === null || listener === undefined) {
      return;
    }
    const named = String(type);
    const held = this[LISTENERS].get(named) ?? [];
    if (!held.some((each) => each.listener === listener)) {
      const once = typeof options === "object" && options !== null && Boolean(options.once);
      held.push({ listener, once });
    }
    this[LISTENERS].set(named, held);
    // `{ signal }` is the way a listener is removed without being held
    // on to, and a library that passes one expects the listener to go
    // when the controller is aborted rather than to stay.
    const signal = typeof options === "object" && options !== null ? options.signal : undefined;
    if (signal !== undefined && signal !== null) {
      signal.addEventListener("abort", () => unlistened.call(this, named, listener));
    }
  }

  function unlistened(type, listener) {
    const held = this[LISTENERS].get(String(type));
    if (held === undefined) {
      return;
    }
    const at = held.findIndex((each) => each.listener === listener);
    if (at !== -1) {
      held.splice(at, 1);
    }
  }

  /// The thing a library extends when it wants to emit events of its
  /// own, which is most of them: an sdk that reports its own retries,
  /// a redis client, a stripe client. There is no tree here, so this
  /// is one object dispatching to its own listeners and nothing else.
  ///
  /// The three methods take the global when they are called without a
  /// receiver, which is the rule the web platform has for an interface
  /// on the global object and is why `addEventListener("x", f)` on its
  /// own works in a module: the global is an event target, these are
  /// its methods, and a call with no dot in front of it has no `this`
  /// in module code.
  class EventTarget {
    constructor() {
      listens(this);
    }

    addEventListener(type, listener, options = {}) {
      listened.call(this ?? globalThis, type, listener, options);
    }

    removeEventListener(type, listener) {
      unlistened.call(this ?? globalThis, type, listener);
    }

    dispatchEvent(event) {
      return fires(this ?? globalThis, event);
    }
  }

  // ---------------------------------------------------------------
  // AbortController
  //
  // Here because the older way of serving needs it: `new Server(...)`
  // in `std/http/server.ts` makes a controller in a field initializer,
  // so a function written the way the older examples are written cannot
  // even be constructed without one. It is the shape rather than the
  // plumbing: nothing here cancels a fetch yet, and `docs/functions.md`
  // says so.

  const SIGNAL = Symbol("signal");
  const REASON = Symbol("reason");

  /// The error an abort is reported as, and the one thing anything
  /// catching an abort tests for by name.
  class DOMException extends Error {
    constructor(message = "", name = "Error") {
      super(message);
      this.name = name;
    }
  }

  class AbortSignal extends EventTarget {
    constructor(guard) {
      if (guard !== SIGNAL) {
        throw new TypeError("an AbortSignal is made by an AbortController");
      }
      super();
      this[REASON] = undefined;
      this.onabort = null;
    }

    get aborted() {
      return this[REASON] !== undefined;
    }

    get reason() {
      return this[REASON];
    }

    throwIfAborted() {
      if (this.aborted) {
        throw this[REASON];
      }
    }

  }

  class AbortController {
    constructor() {
      this[SIGNAL] = new AbortSignal(SIGNAL);
    }

    get signal() {
      return this[SIGNAL];
    }

    abort(reason) {
      const signal = this[SIGNAL];
      if (signal.aborted) {
        return;
      }
      // The default reason is the one everything catching an abort is
      // written against, `err.name === "AbortError"`.
      signal[REASON] = reason === undefined
        ? new DOMException("The signal has been aborted", "AbortError")
        : reason;
      fires(signal, new Event("abort"));
    }
  }

  // ---------------------------------------------------------------
  // WebSocket
  //
  // The handshake, the frames and the close are the host's, because
  // they are a protocol and the crate that speaks it is already in this
  // build. What is here is the object: a state machine of four states,
  // four events, and a read loop that turns what arrived into one of
  // them.
  //
  // A socket lives as long as the call does. The isolate ends with the
  // answer plus whatever `EdgeRuntime.waitUntil` is still waiting for,
  // so a function that wants to hear back from a socket has to be
  // waiting on it when it answers, or to have said so with waitUntil.

  const SOCKET = Symbol("socket");
  const READY = Symbol("readyState");
  const BINARY = Symbol("binaryType");

  const CONNECTING = 0;
  const OPEN = 1;
  const CLOSING = 2;
  const CLOSED = 3;

  class WebSocket extends EventTarget {
    constructor(url, protocols = []) {
      super();
      this.onopen = null;
      this.onmessage = null;
      this.onerror = null;
      this.onclose = null;
      const named =
        protocols === undefined || protocols === null
          ? []
          : Array.isArray(protocols)
            ? protocols.map(String)
            : [String(protocols)];
      // The spec's rewrite, so a project that keeps one url in an
      // environment variable does not need two.
      const asked = new URL(String(url));
      if (asked.protocol === "http:") {
        asked.protocol = "ws:";
      } else if (asked.protocol === "https:") {
        asked.protocol = "wss:";
      }
      if (asked.protocol !== "ws:" && asked.protocol !== "wss:") {
        throw new TypeError(`${asked.protocol} is not a scheme a websocket is opened on`);
      }
      if (asked.hash !== "") {
        throw new TypeError("a websocket url may not have a fragment on it");
      }
      this.url = asked.href;
      this.protocol = "";
      this.extensions = "";
      this.bufferedAmount = 0;
      this[BINARY] = "blob";
      this[READY] = CONNECTING;
      this[SOCKET] = null;
      opening(this, this.url, named);
    }

    get readyState() {
      return this[READY];
    }

    get binaryType() {
      return this[BINARY];
    }

    set binaryType(kind) {
      if (kind !== "blob" && kind !== "arraybuffer") {
        throw new TypeError(`${kind} is not a binaryType`);
      }
      this[BINARY] = kind;
    }


    send(data) {
      if (this[READY] === CONNECTING) {
        throw new TypeError("the socket is still connecting");
      }
      if (this[READY] !== OPEN) {
        // The spec's answer for a closed socket is to count the bytes
        // and drop them rather than to throw.
        return;
      }
      const id = this[SOCKET];
      if (typeof data === "string") {
        ops.op_zou_ws_send_text(id, data).catch((thrown) => broke(this, thrown));
        return;
      }
      if (data instanceof Blob) {
        // A blob is bytes that have not been read yet, which is the one
        // send that cannot happen in this turn.
        data.bytes().then(
          (bytes) => ops.op_zou_ws_send_bytes(id, bytes),
          (thrown) => broke(this, thrown),
        ).catch((thrown) => broke(this, thrown));
        return;
      }
      ops.op_zou_ws_send_bytes(id, bytesOf(data)).catch((thrown) => broke(this, thrown));
    }

    close(code, reason = "") {
      if (code !== undefined && code !== 1000 && (code < 3000 || code > 4999)) {
        throw new TypeError(`${code} is not a code a websocket may be closed with`);
      }
      if (this[READY] === CLOSING || this[READY] === CLOSED) {
        return;
      }
      if (this[READY] === CONNECTING) {
        // Nothing to send a frame on yet. The connection is abandoned
        // as soon as it opens, which is what the spec calls failing it.
        this[READY] = CLOSING;
        return;
      }
      this[READY] = CLOSING;
      ops
        .op_zou_ws_close(this[SOCKET], code ?? 1000, String(reason))
        .catch((thrown) => broke(this, thrown));
    }
  }

  for (const holder of [WebSocket, WebSocket.prototype]) {
    Object.defineProperties(holder, {
      CONNECTING: { value: CONNECTING },
      OPEN: { value: OPEN },
      CLOSING: { value: CLOSING },
      CLOSED: { value: CLOSED },
    });
  }

  async function opening(socket, url, protocols) {
    let opened;
    try {
      opened = await ops.op_zou_ws_connect(url, protocols);
    } catch (thrown) {
      // A handshake that failed is an error and then a close, in that
      // order, with the code that means the connection went away
      // without one being agreed.
      broke(socket, thrown);
      gone(socket, 1006, "", false);
      return;
    }
    socket[SOCKET] = opened.id;
    socket.protocol = opened.protocol;
    socket.extensions = opened.extensions;
    if (socket[READY] === CLOSING) {
      // `close()` was called while this was still connecting, so the
      // socket is opened and immediately given up.
      ops.op_zou_ws_close(opened.id, 1000, "").catch(() => {});
    } else {
      socket[READY] = OPEN;
      fires(socket, new Event("open"));
    }
    await reading(socket, opened.id);
  }

  async function reading(socket, id) {
    for (;;) {
      let arrived;
      try {
        arrived = await ops.op_zou_ws_next(id);
      } catch (thrown) {
        broke(socket, thrown);
        gone(socket, 1006, "", false);
        return;
      }
      if (arrived.kind === "close") {
        gone(socket, arrived.code, arrived.reason, arrived.code !== 1006);
        return;
      }
      if (socket[READY] !== OPEN) {
        // Something arrived after this end asked to close, which is
        // ordinary and is not something a handler is told about.
        continue;
      }
      const data =
        arrived.kind === "text"
          ? arrived.text
          : socket[BINARY] === "arraybuffer"
            ? arrived.bytes.buffer
            : new Blob([arrived.bytes]);
      fires(socket, new MessageEvent("message", { data, origin: socket.url }));
    }
  }

  function broke(socket, thrown) {
    fires(
      socket,
      new ErrorEvent("error", {
        message: thrown instanceof Error ? thrown.message : String(thrown),
        error: thrown,
      }),
    );
  }

  function gone(socket, code, reason, wasClean) {
    if (socket[READY] === CLOSED) {
      return;
    }
    socket[READY] = CLOSED;
    if (socket[SOCKET] !== null) {
      ops.op_zou_ws_drop(socket[SOCKET]);
    }
    fires(socket, new CloseEvent("close", { code, reason, wasClean }));
  }

  // ---------------------------------------------------------------
  // EdgeRuntime.waitUntil
  //
  // Work that outlives the answer. What is registered here is held
  // until it settles, after the caller has already been answered, and
  // the host is what decides how long that is allowed to take.

  const waiting = new Set();

  function waitUntil(work) {
    // A rejection here has nobody left to catch it: whatever
    // registered the work returned before it failed and the answer is
    // already on its way, so it is logged and it is not an error the
    // caller can be told about.
    waiting.add(Promise.resolve(work).catch((thrown) => console.error(thrown)));
  }

  async function drain() {
    // A loop rather than one wait, because work registered from
    // inside work is still work that was registered.
    while (waiting.size > 0) {
      const held = Array.from(waiting);
      waiting.clear();
      await Promise.allSettled(held);
    }
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
  // Reading a static file
  //
  // The four spellings Deno has, over the two ops that do the reading.
  // What a function may open is its own `static_files` and nothing
  // else, which the op decides, so everything here is turning a name
  // into a string and an answer into a value or the error Deno would
  // have thrown.

  class NotFound extends Error {
    constructor(message) {
      super(message);
      this.name = "NotFound";
    }
  }

  class PermissionDenied extends Error {
    constructor(message) {
      super(message);
      this.name = "PermissionDenied";
    }
  }

  /// The five `std/http/server.ts` tests an accept failure against by
  /// name, and the one it throws itself when a closed server is asked
  /// to serve. None of them is ever raised here, because there is no
  /// socket to fail, but `error instanceof Deno.errors.BadResource` on
  /// an `undefined` is a TypeError, so the names have to be there for
  /// the path that never runs.
  function named(name) {
    const raised = class extends Error {
      constructor(message) {
        super(message);
        this.name = name;
      }
    };
    Object.defineProperty(raised, "name", { value: name });
    return raised;
  }

  const BadResource = named("BadResource");
  const InvalidData = named("InvalidData");
  const UnexpectedEof = named("UnexpectedEof");
  const ConnectionReset = named("ConnectionReset");
  const NotConnected = named("NotConnected");
  const Http = named("Http");
  const Interrupted = named("Interrupted");
  const BrokenPipe = named("BrokenPipe");

  /// A `string | URL`, which is what Deno takes, as the string the op
  /// wants. A file url is the path in it, since that is the only thing
  /// a file url is here.
  function pathOf(path) {
    if (path instanceof URL) {
      if (path.protocol !== "file:") {
        throw new TypeError(`a file may only be read through a file url, not ${path.protocol}`);
      }
      return decodeURIComponent(path.pathname);
    }
    return String(path);
  }

  function fileOf(read) {
    switch (read.kind) {
      case "bytes":
        return read.bytes;
      case "refused":
        throw new PermissionDenied(read.why);
      case "missing":
        throw new NotFound(read.why);
      default:
        throw new Error(read.why);
    }
  }

  function readFile(path) {
    return ops.op_zou_read_file(pathOf(path)).then(fileOf);
  }

  function readFileSync(path) {
    return fileOf(ops.op_zou_read_file_sync(pathOf(path)));
  }

  async function readTextFile(path) {
    return new TextDecoder().decode(await readFile(path));
  }

  function readTextFileSync(path) {
    return new TextDecoder().decode(readFileSync(path));
  }

  // ---------------------------------------------------------------
  // Deno.permissions
  //
  // A library asks this before it reaches for something it can do
  // without, so what it is for is a library deciding rather than a
  // function being stopped: `@sentry/deno` calls `querySync` while its
  // sdk is being set up and a runtime with no `Deno.permissions` at all
  // is a `TypeError` at the top of the module.
  //
  // Upstream answers `granted` to all eight names, measured on a real
  // `supabase start` rather than guessed, and a worker there can no
  // more spawn a process than one here can. This says `denied` to the
  // three that are not here, because a library told `granted` and then
  // handed a `TypeError` is worse off than one told no, and the
  // difference is written down in `docs/functions.md`.
  //
  // Nothing here can be revoked either. A function's permissions are
  // the runtime's rather than the function's, so `revoke` answers what
  // `query` answers rather than pretending to take something away it is
  // not enforcing.

  const ALLOWED = {
    // The environment is readable, the network is reachable, and a
    // static file is readable, which is the whole of what a function
    // is given.
    env: "granted",
    net: "granted",
    read: "granted",
    // `performance.now()` is here, at its full resolution.
    hrtime: "granted",
    // Nothing here writes a file, starts a process, opens a library or
    // asks the host about itself.
    write: "denied",
    run: "denied",
    ffi: "denied",
    sys: "denied",
  };

  const STATE = Symbol("state");

  class PermissionStatus extends EventTarget {
    constructor(state) {
      super();
      this[STATE] = state;
      this.onchange = null;
      // Whether the answer covers less than what was asked for, which
      // it never does here: there is no allow list to be partly on.
      this.partial = false;
    }
    get state() {
      return this[STATE];
    }
  }

  function permission(descriptor) {
    const name = descriptor === null || descriptor === undefined ? undefined : descriptor.name;
    const state = ALLOWED[name];
    if (state === undefined) {
      throw new TypeError(`${String(name)} is not a permission a function can ask about`);
    }
    return new PermissionStatus(state);
  }

  const permissions = {
    querySync: permission,
    query(descriptor) {
      return promised(() => permission(descriptor));
    },
    // Asking is the same as knowing, because there is nobody to prompt.
    requestSync: permission,
    request(descriptor) {
      return promised(() => permission(descriptor));
    },
    revokeSync: permission,
    revoke(descriptor) {
      return promised(() => permission(descriptor));
    },
  };

  // The value or the throw, as a promise either way, which is what the
  // async half of that object is.
  function promised(get) {
    try {
      return Promise.resolve(get());
    } catch (e) {
      return Promise.reject(e);
    }
  }

  // ---------------------------------------------------------------
  // What the module sees

  const EdgeRuntime = { waitUntil };

  Object.assign(globalThis, {
    AbortController,
    AbortSignal,
    Blob,
    CloseEvent,
    DOMException,
    CryptoKey,
    CustomEvent,
    EdgeRuntime,
    ErrorEvent,
    Event,
    EventTarget,
    MessageEvent,
    WebSocket,
    File,
    FormData,
    Headers,
    Request,
    Response,
    TextDecoder,
    TextEncoder,
    ReadableStream,
    ReadableStreamDefaultReader,
    TransformStream,
    WritableStream,
    WritableStreamDefaultWriter,
    URL,
    URLSearchParams,
    atob,
    btoa,
    clearInterval: clearTimer,
    clearTimeout: clearTimer,
    console,
    crypto,
    fetch,
    navigator,
    performance,
    queueMicrotask,
    setInterval,
    setTimeout,
  });
  // The global is itself an event target, which is not decoration: a
  // library calls the bare `addEventListener` at the top of a module
  // often enough that a runtime without one is a ReferenceError before
  // the function has a handler. `elevenlabs` is the one in the corpus.
  //
  // The shape is upstream's, measured through a function on a real
  // `supabase start`: `globalThis instanceof EventTarget` is true
  // there, the `addEventListener` on it is the one on
  // `EventTarget.prototype` rather than a copy, and `self` and `window`
  // are both the global itself. `window` was `undefined` here, which
  // was a guess about Deno 2 having removed it, and the runtime a
  // function actually runs on has it. A package deciding whether it is
  // in a browser by asking for `window` now takes the same branch on
  // both servers, which is the whole point.
  Object.setPrototypeOf(globalThis, EventTarget.prototype);
  listens(globalThis);
  globalThis.self = globalThis;
  globalThis.window = globalThis;

  Object.assign(Deno, {
    serve,
    env,
    readFile,
    readFileSync,
    readTextFile,
    readTextFileSync,
    listen,
    serveHttp,
    // The two a function catching one of them by name is written
    // against, which is what makes a missing file and a file it may
    // not have two different things to it, and the eight the older way
    // of serving names in a catch or throws itself.
    errors: {
      NotFound,
      PermissionDenied,
      BadResource,
      InvalidData,
      UnexpectedEof,
      ConnectionReset,
      NotConnected,
      Http,
      Interrupted,
      BrokenPipe,
    },
    // Enough of it that a function branching on the platform gets an
    // answer rather than an exception.
    build: { target: "unknown", arch: "unknown", os: "linux", vendor: "unknown" },
    // Three strings a function may read, in the shape upstream's
    // runtime says them: what is running, then the v8 it is running on
    // and the typescript its transpiler takes.
    version: ops.op_zou_version(),
    permissions,
    PermissionStatus,
    exit() {
      throw new TypeError("a function may not exit the process it is running in");
    },
  });

  // ---------------------------------------------------------------
  // The entry point, which is the value of this whole file

  async function run(exported) {
    // Three ways a module says what to run, in the order upstream picks
    // between them, measured a pair at a time on a module that did two
    // of them at once: `Deno.serve` beats both of the others and a
    // listener beats a default export.
    //
    // Which is one rule rather than three. Upstream is a socket: a
    // module that took the socket is the module that is served, whether
    // it took it through `Deno.serve` or through the older `serve()`
    // out of `std/http/server.ts`, and the default export is what is
    // left when nobody took it.
    //
    // The listener is a loop rather than a handler, so the request goes
    // into it and the answer comes back out of `respondWith`.
    const fetches =
      exported !== null && exported !== undefined && typeof exported.fetch === "function"
        ? exported.fetch.bind(exported)
        : null;
    // Upstream has no answer for a module that did none of the three:
    // it holds the request until the wall clock and kills the worker
    // with "request has been cancelled by supervisor", so the developer
    // is told a timeout rather than what is wrong. This says it.
    if (handler === null && fetches === null && !accepting.listening) {
      throw new TypeError(
        "the function did not say what to run: no Deno.serve, no default export with a fetch, no listener",
      );
    }
    const call = ops.op_zou_call();
    const request = new Request(call.url, {
      method: call.method,
      headers: call.headers,
      body: call.method === "GET" || call.method === "HEAD" ? undefined : call.body,
    });
    const info = { remoteAddr: { transport: "tcp", hostname: call.peer, port: 0 } };
    const answer =
      handler !== null
        ? await handler(request, info)
        : accepting.listening
          ? await accepted(request)
          : await fetches(request, info);
    if (!(answer instanceof Response)) {
      throw new TypeError("a handler must return a Response");
    }
    const headers = Array.from(answer.headers.entries());
    const streaming = answer[SOURCE];
    if (streaming === null || streaming === undefined) {
      ops.op_zou_answer(answer.status, headers, await sending(answer));
      return;
    }
    // A response that was built out of a stream is sent as it is made:
    // the head goes now, and every chunk goes as it is enqueued. The
    // host is waiting on this promise, so the answer is out of here
    // long before the promise resolves, which is the difference
    // between a caller reading tokens and a caller waiting for them.
    ops.op_zou_answer_start(answer.status, headers);
    const reader = streaming.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) {
          break;
        }
        await ops.op_zou_chunk(chunkOf(value));
      }
      ops.op_zou_chunk_end();
    } catch (why) {
      // Too late for a status code: the caller is already reading a
      // 200. The body stops where it is, which is what an http client
      // is shown when a chunked body ends early.
      ops.op_zou_chunk_fail(String(why?.message ?? why));
    }
  }

  /// One chunk on its way out. The spec is narrow here and so is this:
  /// a body stream gives buffers, a string is a TypeError rather than
  /// something to quietly encode, and a handler that meant text is one
  /// TextEncoder away from having meant buffers.
  function chunkOf(value) {
    if (value instanceof Uint8Array || value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
      return bytesOf(value);
    }
    throw new TypeError("a response body stream may only enqueue buffers");
  }

  // Two of them: the call, and whatever the call left running. The
  // host calls the first, takes the answer, and only then calls the
  // second, which is the whole of what `waitUntil` means.
  return [run, drain];
})(globalThis);
