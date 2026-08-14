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
      throw new TypeError("ReadableStream is not implemented yet");
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
    /// The body's own content type is the blob's, because that is the
    /// only place the type of some bytes is written down here.
    async blob() {
      return new Blob([readBody(this)], { type: this.headers.get("content-type") ?? "" });
    },
    async formData() {
      const type = this.headers.get("content-type") ?? "";
      const boundary = boundaryOf(type);
      if (boundary !== null) {
        return formOf(readBody(this), boundary);
      }
      if (type.split(";")[0].trim().toLowerCase() === "application/x-www-form-urlencoded") {
        const form = new FormData();
        for (const [name, value] of pairsOf(core.decode(readBody(this)))) {
          form.append(name, value);
        }
        return form;
      }
      throw new TypeError("Body can not be decoded as form data");
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
    if (body instanceof Blob) {
      return [body[BLOB].slice(), body.type === "" ? null : body.type];
    }
    if (body instanceof FormData) {
      return multipart(body);
    }
    if (body instanceof URLSearchParams) {
      return [encoder.encode(body.toString()), "application/x-www-form-urlencoded;charset=UTF-8"];
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
        // A request's url is a url, so it is parsed here and not left
        // as whatever string it arrived as: `new Request("/one")` has
        // nothing to be relative to and is an error, the same as Deno.
        this.url = new URL(input).href;
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
    // The Request constructor already refused what is not a url, so
    // what is left to say is which schemes are served.
    const scheme = new URL(request.url).protocol;
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
    stopImmediatePropagation() {}
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

  const LISTENERS = Symbol("listeners");

  function listens(target) {
    target[LISTENERS] = new Map();
  }

  function fires(target, event) {
    event.target = target;
    event.currentTarget = target;
    const named = `on${event.type}`;
    if (typeof target[named] === "function") {
      try {
        target[named].call(target, event);
      } catch (thrown) {
        console.error(thrown);
      }
    }
    for (const listener of target[LISTENERS].get(event.type) ?? []) {
      try {
        if (typeof listener === "function") {
          listener.call(target, event);
        } else {
          listener.handleEvent(event);
        }
      } catch (thrown) {
        // One listener that throws is not the rest of them, and there
        // is nobody above this to catch it either way.
        console.error(thrown);
      }
    }
  }

  function listened(type, listener) {
    if (listener === null || listener === undefined) {
      return;
    }
    const named = String(type);
    const held = this[LISTENERS].get(named) ?? [];
    if (!held.includes(listener)) {
      held.push(listener);
    }
    this[LISTENERS].set(named, held);
  }

  function unlistened(type, listener) {
    const held = this[LISTENERS].get(String(type));
    if (held === undefined) {
      return;
    }
    const at = held.indexOf(listener);
    if (at !== -1) {
      held.splice(at, 1);
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

  class WebSocket {
    constructor(url, protocols = []) {
      listens(this);
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

    addEventListener(type, listener) {
      listened.call(this, type, listener);
    }

    removeEventListener(type, listener) {
      unlistened.call(this, type, listener);
    }

    dispatchEvent(event) {
      fires(this, event);
      return true;
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
  // What the module sees

  const EdgeRuntime = { waitUntil };

  Object.assign(globalThis, {
    Blob,
    CloseEvent,
    CryptoKey,
    EdgeRuntime,
    ErrorEvent,
    Event,
    MessageEvent,
    WebSocket,
    File,
    FormData,
    Headers,
    Request,
    Response,
    TextDecoder,
    TextEncoder,
    ReadableStream: ReadableStreamStub,
    URL,
    URLSearchParams,
    atob,
    btoa,
    clearInterval: clearTimer,
    clearTimeout: clearTimer,
    console,
    crypto,
    fetch,
    queueMicrotask,
    setInterval,
    setTimeout,
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

  // Two of them: the call, and whatever the call left running. The
  // host calls the first, takes the answer, and only then calls the
  // second, which is the whole of what `waitUntil` means.
  return [run, drain];
})(globalThis);
