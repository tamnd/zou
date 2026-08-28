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

  // The labels the encoding standard gives the three encodings this
  // has. It is not the standard's whole list, because the rest of that
  // list is legacy single byte pages and each one is a table of 128
  // characters that nothing has asked for yet.
  //
  // utf-16 is here because a wasm module compiled by emscripten decodes
  // its own strings with it: the heap holds utf-16 code units and
  // `new TextDecoder('utf-16le')` is how the glue reads them out.
  const ENCODINGS = new Map([
    ["unicode-1-1-utf-8", "utf-8"],
    ["utf-8", "utf-8"],
    ["utf8", "utf-8"],
    ["csunicode", "utf-16le"],
    ["iso-10646-ucs-2", "utf-16le"],
    ["ucs-2", "utf-16le"],
    ["unicode", "utf-16le"],
    ["unicodefeff", "utf-16le"],
    ["utf-16", "utf-16le"],
    ["utf-16le", "utf-16le"],
    ["unicodefffe", "utf-16be"],
    ["utf-16be", "utf-16be"],
  ]);

  const ENCODING = Symbol("encoding");
  const KEEP_BOM = Symbol("ignoreBOM");
  const HELD = Symbol("held");

  /// Two bytes at a time, in chunks, because `String.fromCharCode` of a
  /// million arguments is a stack that has run out rather than a string.
  ///
  /// An odd byte at the end is half of a code unit, which is what the
  /// standard's decoder ends on the replacement character for.
  function utf16(bytes, big) {
    const units = bytes.length >> 1;
    const chunk = 4096;
    let out = "";
    for (let start = 0; start < units; start += chunk) {
      const stop = Math.min(units, start + chunk);
      const codes = new Array(stop - start);
      for (let unit = start; unit < stop; unit++) {
        const first = bytes[unit * 2];
        const second = bytes[unit * 2 + 1];
        codes[unit - start] = big ? (first << 8) | second : (second << 8) | first;
      }
      out += String.fromCharCode.apply(null, codes);
    }
    return bytes.length % 2 === 0 ? out : `${out}\uFFFD`;
  }

  class TextDecoder {
    constructor(label = "utf-8", options = {}) {
      const encoding = ENCODINGS.get(String(label).trim().toLowerCase());
      if (encoding === undefined) {
        throw new RangeError(`the encoding label provided ('${label}') is not supported`);
      }
      this[ENCODING] = encoding;
      this[KEEP_BOM] = Boolean(options && options.ignoreBOM);
      this[HELD] = null;
    }
    get encoding() {
      return this[ENCODING];
    }
    // `stream: true` is what makes a decoder usable on a body arriving
    // in chunks: a character whose bytes straddle two of them is held
    // until the rest of it turns up rather than being decoded into the
    // replacement character. Without it, decoding a chunked response a
    // piece at a time quietly corrupts every multi byte character that
    // lands on a boundary, which is not something the caller can see.
    decode(input, options) {
      const streaming = Boolean(options && options.stream);
      let bytes = input === undefined ? EMPTY : bytesOf(input);
      const held = this[HELD];
      if (held !== null) {
        const both = new Uint8Array(held.length + bytes.length);
        both.set(held);
        both.set(bytes, held.length);
        bytes = both;
        this[HELD] = null;
      }
      if (streaming) {
        const cut = whole(bytes, this[ENCODING]);
        if (cut < bytes.length) {
          this[HELD] = bytes.slice(cut);
          bytes = bytes.subarray(0, cut);
        }
      }
      if (bytes.length === 0) {
        return "";
      }
      if (this[ENCODING] === "utf-8") {
        return core.decode(bytes);
      }
      const text = utf16(bytes, this[ENCODING] === "utf-16be");
      // A byte order mark is what said which of the two this is, so it
      // is not part of what it said, unless the caller asked to be
      // handed the bytes as they are.
      return this[KEEP_BOM] || !text.startsWith("\uFEFF") ? text : text.slice(1);
    }
  }

  const EMPTY = new Uint8Array(0);

  /// How much of these bytes is characters that are all here, for a
  /// decoder that has been told more is coming.
  ///
  /// utf-8 says how long a character is in its first byte, so the only
  /// question is whether the last one started and did not finish. utf-16
  /// is two bytes at a time, and a lone surrogate at the end is left to
  /// be decoded as one rather than held, which is what a decoder does
  /// with an unpaired one anyway.
  function whole(bytes, encoding) {
    const end = bytes.length;
    if (encoding !== "utf-8") {
      return end - (end % 2);
    }
    for (let back = 1; back <= 3 && back <= end; back += 1) {
      const byte = bytes[end - back];
      if (byte < 0x80) {
        return end;
      }
      if (byte >= 0xc0) {
        const needs = byte >= 0xf0 ? 4 : byte >= 0xe0 ? 3 : 2;
        return back < needs ? end - back : end;
      }
    }
    return end;
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
  const NAME = Symbol("name");
  const MODIFIED = Symbol("lastModified");

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
      this[NAME] = String(name);
      const given = options === null || options === undefined ? undefined : options.lastModified;
      this[MODIFIED] = given === undefined ? Date.now() : Number(given);
    }

    // Both of these are getters over a symbol rather than the plain
    // fields they were, because a `File` upstream has nothing of its
    // own that a for-in, an `Object.keys`, a `JSON.stringify` or a
    // `structuredClone` can see, and the two were the only place in
    // this file where what a platform object holds was visible from
    // outside it. Measured on a real `supabase start`, where the name
    // is on the prototype and `JSON.stringify(file)` is `{}`.
    get name() {
      return this[NAME];
    }

    get lastModified() {
      return this[MODIFIED];
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
      // Always a signal, never null, because that is what a caller
      // passing `request.signal` on to something else is written
      // against. A request nobody gave one to gets one nothing aborts.
      //
      // A request given one gets its own that follows it rather than
      // the one it was handed, which is measurable from a function:
      // `new Request(url, { signal }).signal === signal` is false on a
      // real `supabase start` and aborting the controller still aborts
      // the request. `any` of the one is what following is.
      const given = init.signal ?? (input instanceof Request ? input.signal : undefined);
      this.signal = given === undefined || given === null
        ? new AbortController().signal
        : AbortSignal.any([given]);
      this[USED] = false;
    }

    clone() {
      const copy = new Request(this.url, {
        method: this.method,
        headers: this.headers,
        signal: this.signal,
      });
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
    // A caller that has already given up is a call that is never made,
    // which is the one part of a signal a client cannot get wrong.
    const signal = request.signal;
    if (signal.aborted) {
      throw signal.reason;
    }
    // Reading the body is what sending it is, and a request whose body
    // was already read is a request with nothing left to send.
    const body = await readBody(request);
    const id = next();
    const sent = ops.op_zou_fetch(
      {
        method: request.method,
        url: request.url,
        headers: Array.from(request.headers.entries()),
      },
      body,
      id,
    );
    // Nobody awaits the loser of a race, and an op that rejects after
    // its caller has gone is an unhandled rejection rather than an
    // error anybody can act on. Attached here rather than below the
    // race because the abort a body took long enough to miss leaves
    // this promise with nobody on it either.
    sent.catch(() => {});
    // Reading the body is the one await between the check above and
    // the call, so this is the caller that gave up while it happened.
    if (signal.aborted) {
      ops.op_zou_fetch_abort(id);
      throw signal.reason;
    }
    // The abort is two halves. The caller's promise rejects with the
    // reason the moment the signal says so, which is what a library
    // waiting on this is written against, and the op is told so the
    // isolate is not left holding a call nobody wants. What the op
    // cannot do is take the connection down: the request is on a
    // blocking thread inside a client with no handle to it, so it runs
    // to its own end and its answer is dropped. `docs/functions.md`
    // says so rather than leaving it to be found.
    let stop = null;
    const abort = new Promise((_, broken) => {
      stop = () => {
        ops.op_zou_fetch_abort(id);
        broken(signal.reason);
      };
      signal.addEventListener("abort", stop, { once: true });
    });
    abort.catch(() => {});
    try {
      return received(await Promise.race([sent, abort]));
    } finally {
      signal.removeEventListener("abort", stop);
    }
  }

  // Which call is which, so that ending one ends that one. Per
  // isolate, because the op state holding the other end is, and back
  // round at a number v8 still keeps in a register: an isolate with
  // this many calls behind it has none of the first one left.
  let calls = 0;

  function next() {
    calls = (calls + 1) & 0x3fffffff;
    return calls;
  }

  // ---------------------------------------------------------------
  // wasm from a response
  //
  // Here rather than beside the rest of WebAssembly, which is v8's and
  // needs nothing from anybody, because the two streaming calls are
  // the two that take a Response and so are the two this has to write.
  //
  // v8's own instantiateStreaming asks the embedder for the bytes
  // through a callback, deno_core forwards that to a javascript
  // handler, and a runtime that never set one aborts the process the
  // moment a function reaches the call. Not throws, aborts: every
  // function on the node after it answers nothing, from three lines of
  // one tenant's code. See #592.
  //
  // So the call never reaches v8's. Read the response, hand the bytes
  // to the call that takes bytes. That is not streaming, and a module
  // is compiled after it arrives rather than while it does, which
  // costs a copy of the module and nothing else. What a package wants
  // here is for the call to work.

  /// The bytes behind whatever a streaming call was handed, with the
  /// checks the spec puts before them.
  ///
  /// A promise is awaited first because both calls take either, which
  /// is the whole reason `instantiateStreaming(fetch(url))` reads the
  /// way it does. The content type is a real requirement rather than
  /// politeness: a server that answers a wasm url with an html error
  /// page is the ordinary failure here, and without the check it
  /// arrives as a compile error about a magic number.
  async function wasmBytes(source, called) {
    const response = await source;
    if (!(response instanceof Response)) {
      throw new TypeError(`WebAssembly.${called} takes a Response or a promise of one`);
    }
    const type = (response.headers.get("content-type") || "").split(";")[0].trim();
    if (type.toLowerCase() !== "application/wasm") {
      throw new TypeError(
        `WebAssembly.${called} needs a response of type application/wasm, this one is ${
          type || "of no type"
        }`,
      );
    }
    if (!response.ok) {
      throw new TypeError(
        `WebAssembly.${called} got ${response.status} from ${response.url || "the response"}`,
      );
    }
    return await response.arrayBuffer();
  }

  WebAssembly.compileStreaming = async function compileStreaming(source) {
    return await WebAssembly.compile(await wasmBytes(source, "compileStreaming"));
  };

  WebAssembly.instantiateStreaming = async function instantiateStreaming(source, imports) {
    return await WebAssembly.instantiate(
      await wasmBytes(source, "instantiateStreaming"),
      imports,
    );
  };

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

  /// The name an algorithm was asked for by, out of a string or out of
  /// the object that carries the rest of the parameters with it.
  function nameOf(algorithm) {
    return algorithm !== null && typeof algorithm === "object"
      ? String(algorithm.name ?? "")
      : String(algorithm);
  }

  /// The hash an HMAC operation is under, which is the key's and not
  /// the algorithm's: `sign("HMAC", key, data)` names no hash at all.
  function hmacHash(algorithm, key) {
    const named = nameOf(algorithm);
    if (named.toLowerCase() !== "hmac") {
      throw new TypeError(`${named} is not supported yet, only HMAC is`);
    }
    if (!(key instanceof CryptoKey)) {
      throw new TypeError("a key is required");
    }
    return key.algorithm.hash.name;
  }

  /// The ciphers a key can be made for, spelled the way the spec spells
  /// them so that a key made for one is refused by the other.
  const CIPHERS = ["AES-CBC", "AES-GCM"];

  function cipherNamed(algorithm) {
    const given = nameOf(algorithm);
    return CIPHERS.find((name) => name.toLowerCase() === given.toLowerCase());
  }

  /// What an AES key says about itself, and the one check worth making
  /// while it is being made: there are three key lengths and a key of
  /// any other length is not a key, whichever end it came from.
  function aesAlgorithm(cipher, length) {
    if (length !== 128 && length !== 192 && length !== 256) {
      throw new DOMException(`an AES key is 128, 192 or 256 bits and this one is ${length}`, "DataError");
    }
    return { name: cipher, length };
  }

  /// A key holds its bytes and the algorithm it was made for, and the
  /// algorithm is what every operation asks it about afterwards: an
  /// AES-CBC key handed to `sign` is the wrong key and says so.
  ///
  /// The tag is here because a library that takes keys from callers
  /// checks for it rather than for the class: `jose`, which is what
  /// verifies a JWT in most functions that verify one, decides whether
  /// it was handed a key by asking what it calls itself.
  class CryptoKey {
    constructor(bytes, algorithm, extractable, usages, type) {
      this[SECRET] = bytes;
      this.type = type ?? "secret";
      this.extractable = Boolean(extractable);
      this.usages = usages;
      this.algorithm = algorithm;
    }

    get [Symbol.toStringTag]() {
      return "CryptoKey";
    }
  }

  /// The public half of an asymmetric key, as its two coordinates. A
  /// private key carries one too, derived from the scalar, so a key
  /// imported from a jwk with only `d` in it can still check what it
  /// signed.
  const POINT = Symbol("point");

  /// The one curve, spelled the way a jwk spells it.
  const CURVE = "P-256";

  function isEcdsa(key) {
    return key instanceof CryptoKey && key.algorithm.name === "ECDSA";
  }

  /// The hash an ECDSA call is under, which is the call's rather than
  /// the key's: a P-256 key is not made for one hash, and the caller
  /// names it every time.
  function ecdsaHash(algorithm) {
    const named = nameOf(algorithm);
    if (named.toLowerCase() !== "ecdsa") {
      throw new TypeError(`this key is for ECDSA and the call is for ${named}`);
    }
    return hashNamed(
      algorithm !== null && typeof algorithm === "object" ? algorithm.hash : "SHA-256",
    );
  }

  /// The bytes of a base64url field of a jwk, which is how a jwk holds
  /// every number in it.
  function fromBase64Url(text, called) {
    if (typeof text !== "string" || text.length === 0) {
      throw new DOMException(`the jwk has no ${called}`, "DataError");
    }
    const padded = text.replace(/-/g, "+").replace(/_/g, "/");
    let binary;
    try {
      binary = atob(padded + "=".repeat((4 - (padded.length % 4)) % 4));
    } catch {
      throw new DOMException(`the jwk's ${called} is not base64url`, "DataError");
    }
    const bytes = new Uint8Array(binary.length);
    for (let at = 0; at < binary.length; at += 1) {
      bytes[at] = binary.charCodeAt(at);
    }
    return bytes;
  }

  /// The key an encryption is under, checked against the name the call
  /// made, because a key is made for one cipher and used with one.
  function cipherKey(cipher, key, called) {
    if (!(key instanceof CryptoKey)) {
      throw new TypeError("a key is required");
    }
    if (key.algorithm.name !== cipher) {
      throw new TypeError(
        `this key is for ${key.algorithm.name} and the ${called} is for ${cipher}`,
      );
    }
    return key[SECRET];
  }

  /// The parameters an AES call carries beside the data: the iv, which
  /// both modes need, and for GCM what is authenticated without being
  /// encrypted and how long the tag is.
  function cipherParams(algorithm, cipher) {
    if (algorithm === null || typeof algorithm !== "object") {
      throw new TypeError(`${cipher} needs an iv`);
    }
    const iv = sourceOf(algorithm.iv, "iv");
    if (cipher === "AES-CBC") {
      return { iv, extra: new Uint8Array(0), tag: 0 };
    }
    const extra =
      algorithm.additionalData === undefined
        ? new Uint8Array(0)
        : sourceOf(algorithm.additionalData, "additionalData");
    const tag = algorithm.tagLength === undefined ? 0 : Number(algorithm.tagLength);
    return { iv, extra, tag };
  }

  /// What a failed decryption is, which is one error however it failed:
  /// the host says this sentence and nothing else, and the party
  /// holding the wrong key learns nothing from which part was wrong.
  const FAILED = "Decryption failed";

  function raised(error) {
    if (error instanceof TypeError && error.message === FAILED) {
      return new DOMException(FAILED, "OperationError");
    }
    return error;
  }

  function refuse(name) {
    return () => {
      throw new TypeError(`crypto.subtle.${name} is not supported yet`);
    };
  }

  /// A key made of bytes, which is what both raw keys and the `oct`
  /// half of the jwk format are.
  function symmetricKey(bytes, algorithm, extractable, held) {
    const cipher = cipherNamed(algorithm);
    if (cipher !== undefined) {
      return new CryptoKey(bytes, aesAlgorithm(cipher, bytes.length * 8), extractable, held);
    }
    const named = nameOf(algorithm);
    if (named.toLowerCase() !== "hmac") {
      throw new TypeError(`${named} keys are not supported yet, only HMAC, AES and ECDSA are`);
    }
    const hash = hashNamed(algorithm.hash);
    return new CryptoKey(
      bytes,
      { name: "HMAC", hash: { name: hash }, length: bytes.length * 8 },
      extractable,
      held,
    );
  }

  /// A key out of a jwk: the `oct` shape, which is bytes with a base64
  /// coat on, and the `EC` shape, which is a point and possibly the
  /// scalar behind it.
  ///
  /// A jwks published by a Supabase project is EC and public, one key
  /// per line of the set, and importing one of them is the whole of
  /// what a function does before it checks a token.
  function jwkKey(jwk, algorithm, extractable, held) {
    if (jwk === null || typeof jwk !== "object") {
      throw new TypeError("a jwk is required");
    }
    const kty = String(jwk.kty ?? "");
    if (kty === "oct") {
      return symmetricKey(fromBase64Url(jwk.k, "k"), algorithm, extractable, held);
    }
    if (kty !== "EC") {
      throw new TypeError(`${kty} keys are not supported yet, only oct and EC are`);
    }
    const named = nameOf(algorithm);
    if (named.toLowerCase() !== "ecdsa") {
      throw new TypeError(`an EC jwk is an ECDSA key and the call asked for ${named}`);
    }
    const curve = String(jwk.crv ?? "");
    if (curve !== CURVE) {
      throw new DOMException(`the only curve here is ${CURVE} and this jwk is on ${curve}`, "DataError");
    }
    const shape = { name: "ECDSA", namedCurve: CURVE };
    // A private jwk carries d and, in a published set, x and y as
    // well. The point is derived from d rather than read, so a jwk
    // whose coordinates disagree with its scalar cannot import as a
    // key that verifies nothing.
    if (jwk.d !== undefined) {
      const scalar = fromBase64Url(jwk.d, "d");
      const point = ops.op_zou_ec_public(scalar);
      const key = new CryptoKey(scalar, shape, extractable, held, "private");
      key[POINT] = { x: point.slice(0, 32), y: point.slice(32) };
      return key;
    }
    const key = new CryptoKey(new Uint8Array(0), shape, extractable, held, "public");
    key[POINT] = { x: fromBase64Url(jwk.x, "x"), y: fromBase64Url(jwk.y, "y") };
    return key;
  }

  const subtle = {
    async digest(algorithm, data) {
      const digested = ops.op_zou_digest(hashNamed(algorithm), sourceOf(data, "data"));
      return digested.buffer;
    },

    /// Raw keys and jwks, which are the two formats a function has: an
    /// HMAC key is bytes and an AES key is bytes, and the key a token
    /// is verified against arrives as a jwk out of a project's
    /// published set. The der formats want a parser this has no reason
    /// to carry, so they are refused by name.
    async importKey(format, keyData, algorithm, extractable, usages) {
      const held = Array.from(usages ?? []).map(String);
      if (String(format) === "jwk") {
        return jwkKey(keyData, algorithm, extractable, held);
      }
      if (String(format) !== "raw") {
        throw new TypeError(`the ${format} key format is not supported yet, only raw and jwk are`);
      }
      return symmetricKey(sourceOf(keyData, "keyData"), algorithm, extractable, held);
    },

    /// A key out of the operating system's randomness, which is where
    /// a key nobody handed in has to come from.
    async generateKey(algorithm, extractable, usages) {
      const cipher = cipherNamed(algorithm);
      if (cipher === undefined) {
        throw new TypeError(`${nameOf(algorithm)} keys cannot be generated yet, only AES can`);
      }
      const length = Number(algorithm?.length);
      const bytes = new Uint8Array(length / 8);
      ops.op_zou_random(bytes);
      return new CryptoKey(
        bytes,
        aesAlgorithm(cipher, length),
        extractable,
        Array.from(usages ?? []).map(String),
      );
    },

    /// The bytes back out, for a key that said it could be, because a
    /// key that is not extractable is one whose bytes are the host's.
    async exportKey(format, key) {
      if (String(format) !== "raw") {
        throw new TypeError(`the ${format} key format is not supported yet, only raw is`);
      }
      if (!(key instanceof CryptoKey)) {
        throw new TypeError("a key is required");
      }
      if (!key.extractable) {
        throw new DOMException("key is not extractable", "InvalidAccessError");
      }
      return key[SECRET].slice().buffer;
    },

    async encrypt(algorithm, key, data) {
      const cipher = cipherNamed(algorithm);
      if (cipher === undefined) {
        throw new TypeError(`${nameOf(algorithm)} is not supported yet, only AES is`);
      }
      const { iv, extra, tag } = cipherParams(algorithm, cipher);
      const said = ops.op_zou_encrypt(
        cipher,
        cipherKey(cipher, key, "encrypt"),
        iv,
        extra,
        tag,
        sourceOf(data, "data"),
      );
      return said.buffer;
    },

    async decrypt(algorithm, key, data) {
      const cipher = cipherNamed(algorithm);
      if (cipher === undefined) {
        throw new TypeError(`${nameOf(algorithm)} is not supported yet, only AES is`);
      }
      const { iv, extra, tag } = cipherParams(algorithm, cipher);
      try {
        const said = ops.op_zou_decrypt(
          cipher,
          cipherKey(cipher, key, "decrypt"),
          iv,
          extra,
          tag,
          sourceOf(data, "data"),
        );
        return said.buffer;
      } catch (error) {
        throw raised(error);
      }
    },

    async sign(algorithm, key, data) {
      if (isEcdsa(key)) {
        if (key.type !== "private") {
          throw new DOMException("a public key signs nothing", "InvalidAccessError");
        }
        const signature = ops.op_zou_ec_sign(
          ecdsaHash(algorithm),
          key[SECRET],
          sourceOf(data, "data"),
        );
        return signature.buffer;
      }
      const hash = hmacHash(algorithm, key);
      const signature = ops.op_zou_sign(hash, key[SECRET], sourceOf(data, "data"));
      return signature.buffer;
    },

    /// The comparison is the host's, because a comparison here would
    /// stop at the first byte that differs and how long a wrong answer
    /// took is how a signature is guessed.
    async verify(algorithm, key, signature, data) {
      if (isEcdsa(key)) {
        const point = key[POINT];
        return ops.op_zou_ec_verify(
          ecdsaHash(algorithm),
          point.x,
          point.y,
          sourceOf(data, "data"),
          sourceOf(signature, "signature"),
        );
      }
      const hash = hmacHash(algorithm, key);
      return ops.op_zou_verify(
        hash,
        key[SECRET],
        sourceOf(data, "data"),
        sourceOf(signature, "signature"),
      );
    },

    deriveBits: refuse("deriveBits"),
    deriveKey: refuse("deriveKey"),
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
  // The sleeping is an op and the bookkeeping is here: what each timer
  // is due at, and the callback it is for. There is one sleep at the
  // host for all of them, for the earliest deadline that has not been
  // reached, and it is retuned whenever that deadline changes. A
  // cleared timer is taken out of the map and the sleep is retuned
  // without it, so a timer set for an hour and cleared is not an hour
  // of a future being held.
  //
  // A sleep per timer is the shorter thing to write and it has an
  // ordering it cannot fix. Four timers set for 0, 10, 20 and 30
  // milliseconds would be four futures, and while the isolate is
  // keeping up they come back one at a time and in order. When the
  // isolate is held up past all four deadlines, which is what a busy
  // node looks like, all four are ready at once and which one the host
  // hands back first is the host's business: measured under load, the
  // thirty came back before the ten. Firing them in that order is wrong
  // however late they all are, and it is the kind of wrong that shows
  // up as a retry that backed off less than the one before it.
  //
  // With one sleep the order is not the host's to decide. Whatever is
  // due when it comes back runs here, earliest deadline first, and it
  // runs inside the op's own turn rather than in a microtask of its
  // own. That last part is not a detail: a callback put off to a
  // microtask leaves the isolate with no op pending and nothing left to
  // do, and a handler waiting on the timer that was about to resolve it
  // is a promise the event loop has already decided nobody will settle.

  const timers = new Map();
  let nextTimer = 1;
  // The id of the sleep that is outstanding, or zero.
  let sleeper = 0;
  // The deadline that sleep is for, so a retune that would ask for
  // the same one again can be skipped.
  let waking = Infinity;

  // The number of milliseconds the host would sleep for, worked out
  // here as well because the deadline is what orders the callbacks. A
  // delay is a signed 32 bit integer in the sense web idl means it, so
  // `| 0` is the whole of the rule: a number past the ceiling wraps
  // rather than being refused, anything that is not a number at all is
  // zero, and everything at or below zero is now.
  function waits(millis) {
    const wait = millis | 0;
    return wait > 0 ? wait : 0;
  }

  function after(callback, delay, args, repeating) {
    if (typeof callback !== "function") {
      // Deno takes a string here and evaluates it. That is `eval` with
      // a longer name, and refusing it is a difference in the direction
      // of no.
      throw new TypeError("a timer needs a function, and a string of code is not one here");
    }
    const id = nextTimer;
    nextTimer += 1;
    const wait = waits(delay);
    timers.set(id, { id, at: ops.op_zou_now() + wait, wait, callback, args, repeating });
    wakes();
    return id;
  }

  // Point the one sleep at the earliest deadline there is.
  function wakes() {
    let earliest = Infinity;
    for (const timer of timers.values()) {
      if (timer.at < earliest) {
        earliest = timer.at;
      }
    }
    if (earliest === waking) {
      return;
    }
    if (sleeper) {
      ops.op_zou_clear(sleeper);
      sleeper = 0;
    }
    waking = earliest;
    if (earliest === Infinity) {
      return;
    }
    // An id of its own rather than the timer's, because which timer the
    // earliest deadline belongs to changes and the sleep is one thing.
    const id = nextTimer;
    nextTimer += 1;
    sleeper = id;
    (async () => {
      // Rounded up, because the host sleeps in whole milliseconds and
      // truncates what it is given: asking for 9.6 and being woken at 9
      // is a timer that fired before its delay was up.
      const wait = Math.ceil(Math.max(0, earliest - ops.op_zou_now()));
      await ops.op_zou_sleep(id, wait);
      if (sleeper !== id) {
        // Retuned while this was sleeping, which is also how a cancel
        // gets here. The sleep that replaced it is the one that counts.
        return;
      }
      sleeper = 0;
      waking = Infinity;
      rings();
    })();
  }

  // Run everything that is due, and go back to sleep for the rest.
  function rings() {
    const now = ops.op_zou_now();
    const ready = [];
    for (const timer of timers.values()) {
      if (timer.at <= now) {
        ready.push(timer);
      }
    }
    // By when each was due, and then by which was made first, which is
    // the order two timers with the same deadline fire in and is what
    // the ids already carry.
    ready.sort((one, other) => one.at - other.at || one.id - other.id);
    for (const timer of ready) {
      if (!timers.has(timer.id)) {
        // Cleared by one of the callbacks that ran ahead of it in this
        // same round, which is a timer that must not fire.
        continue;
      }
      if (timer.repeating) {
        timer.at = now + timer.wait;
      } else {
        timers.delete(timer.id);
      }
      try {
        timer.callback(...timer.args);
      } catch (thrown) {
        // A handler cannot catch this: by the time the timer fires,
        // whatever set it has returned. Deno's answer is to end the
        // process, which here would be to lose an answer that is
        // already written, so this says so and the call goes on.
        reported(thrown);
      }
    }
    // The host sleeps in whole milliseconds and the deadlines here are
    // fractions of one, so a sleep can come back a fraction early and
    // find nothing due. That is not a special case: there is a deadline
    // that has not been reached, and this is the line that waits for
    // it, the same as it does for the timers that were not due yet.
    wakes();
  }

  function setTimeout(callback, delay = 0, ...args) {
    return after(callback, delay, args, false);
  }

  function setInterval(callback, delay = 0, ...args) {
    return after(callback, delay, args, true);
  }

  function clearTimer(id) {
    if (timers.delete(Number(id))) {
      // Retuned rather than cancelled, because the sleep is for the
      // earliest deadline of the timers there are and there may still
      // be some. When there are none it is cancelled in there, which is
      // what gives the hour back to a function that set a timer for an
      // hour and changed its mind.
      wakes();
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
        reported(thrown);
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
  // The entry buffer is here and it is real. It used to be absent, on
  // the grounds that a faked one hands back an empty list to a library
  // that expected to read what it recorded, which is worse than no
  // method at all. That was right about the fake and wrong about the
  // choice: what a library does with `mark` and `measure` is time
  // itself, the recording is a list in the isolate and there is
  // nothing for the host to do in any of it. So it records, and what
  // is read back is what was written.
  //
  // What is not here is the entry types a browser fills in on its own,
  // `resource`, `navigation`, `paint` and the rest. Nothing here
  // navigates or paints, and `PerformanceObserver.supportedEntryTypes`
  // says which two types this is, which is the question a library asks
  // before it observes.

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

  // The brand on an entry the runtime made. `new PerformanceEntry()` is
  // not a thing a program does on the web and it is not one here: an
  // entry comes from a mark or a measure, and the guard is how the two
  // constructors below tell their own call from anybody else's.
  const recorded = Symbol("recorded by the runtime");

  /// One thing that happened, with a name, a kind, when it started and
  /// how long it took. Everything in the buffer is one of these.
  class PerformanceEntry {
    #name;
    #entryType;
    #startTime;
    #duration;

    constructor(guard, name, entryType, startTime, duration) {
      if (guard !== recorded) {
        throw new TypeError("Illegal constructor");
      }
      this.#name = name;
      this.#entryType = entryType;
      this.#startTime = startTime;
      this.#duration = duration;
    }

    get name() {
      return this.#name;
    }

    get entryType() {
      return this.#entryType;
    }

    get startTime() {
      return this.#startTime;
    }

    get duration() {
      return this.#duration;
    }

    toJSON() {
      return {
        name: this.name,
        entryType: this.entryType,
        startTime: this.startTime,
        duration: this.duration,
      };
    }
  }

  /// A moment. Constructible by hand, which is the web's rule and
  /// Deno's: `new PerformanceMark("x")` is an entry that was never
  /// recorded, and only `performance.mark` puts one in the buffer.
  class PerformanceMark extends PerformanceEntry {
    #detail;

    constructor(name, options = {}) {
      const start = options?.startTime === undefined ? ops.op_zou_now() : Number(options.startTime);
      if (!(start >= 0)) {
        throw new TypeError(`a mark cannot start at ${options?.startTime}, which is before the isolate did`);
      }
      super(recorded, String(name), "mark", start, 0);
      this.#detail = options?.detail === undefined ? null : options.detail;
    }

    get detail() {
      return this.#detail;
    }

    toJSON() {
      return { ...super.toJSON(), detail: this.detail };
    }
  }

  /// A span between two moments, which is the thing a library is
  /// actually after when it marks twice.
  class PerformanceMeasure extends PerformanceEntry {
    #detail;

    constructor(guard, name, startTime, duration, detail) {
      super(guard, name, "measure", startTime, duration);
      this.#detail = detail === undefined ? null : detail;
    }

    get detail() {
      return this.#detail;
    }

    toJSON() {
      return { ...super.toJSON(), detail: this.detail };
    }
  }

  // Everything recorded so far, oldest first, which is the order every
  // getter hands it back in.
  const entries = [];

  // The observers that are watching, and what each of them has been
  // handed but not yet told about. A callback runs in a microtask
  // rather than in the call that recorded the entry, so a library that
  // marks inside its own observer callback does not reenter it.
  const observers = new Set();

  // How `record` reaches into an observer without putting the method on
  // the class where a function could call it.
  const handed = Symbol("an entry for an observer");

  /// A time, from either a number or the name of a mark. A library
  /// measures between two names far more often than between two
  /// numbers, and the name means the most recent mark that had it.
  function whenWas(what) {
    if (typeof what === "number") {
      if (!(what >= 0)) {
        throw new TypeError(`${what} is not a time`);
      }
      return what;
    }
    const name = String(what);
    for (let at = entries.length - 1; at >= 0; at -= 1) {
      if (entries[at].entryType === "mark" && entries[at].name === name) {
        return entries[at].startTime;
      }
    }
    throw new DOMException(`nothing was marked ${JSON.stringify(name)}`, "SyntaxError");
  }

  /// Put an entry in the buffer and tell whoever is watching for its
  /// kind.
  function record(entry) {
    entries.push(entry);
    for (const observer of observers) {
      observer[handed](entry);
    }
    return entry;
  }

  /// What an observer's callback is given: the entries since it last
  /// ran, in the same three shapes the buffer itself answers in.
  class PerformanceObserverEntryList {
    #entries;

    constructor(guard, list) {
      if (guard !== recorded) {
        throw new TypeError("Illegal constructor");
      }
      this.#entries = list;
    }

    getEntries() {
      return this.#entries.slice();
    }

    getEntriesByName(name, type) {
      const wanted = String(name);
      return this.#entries.filter(
        (entry) => entry.name === wanted && (type === undefined || entry.entryType === String(type)),
      );
    }

    getEntriesByType(type) {
      const wanted = String(type);
      return this.#entries.filter((entry) => entry.entryType === wanted);
    }
  }

  class PerformanceObserver {
    // The two kinds anything here records. A library asks this before
    // it observes, and the honest answer is short.
    static supportedEntryTypes = ["mark", "measure"];

    #callback;
    #watching = new Set();
    #queued = [];
    #due = false;

    constructor(callback) {
      if (typeof callback !== "function") {
        throw new TypeError("a PerformanceObserver is made with the function it calls");
      }
      this.#callback = callback;
    }

    observe(options = {}) {
      const types = options.entryTypes ?? (options.type === undefined ? [] : [options.type]);
      if (options.entryTypes !== undefined && options.type !== undefined) {
        throw new TypeError("an observer watches either entryTypes or a single type, not both");
      }
      // `entryTypes` replaces what was being watched, a single `type`
      // adds to it, which is the web's rule.
      if (options.entryTypes !== undefined) {
        this.#watching = new Set(Array.from(types, String));
      } else {
        for (const type of types) this.#watching.add(String(type));
      }
      observers.add(this);
      // A buffered observer is told about what was recorded before it
      // started watching, which is how a library that observes after
      // its own setup still sees the setup.
      if (options.buffered) {
        for (const entry of entries) this[handed](entry);
      }
    }

    disconnect() {
      observers.delete(this);
      this.#watching.clear();
      this.#queued = [];
    }

    takeRecords() {
      const held = this.#queued;
      this.#queued = [];
      return held;
    }

    [handed](entry) {
      if (!this.#watching.has(entry.entryType)) return;
      this.#queued.push(entry);
      if (this.#due) return;
      this.#due = true;
      queueMicrotask(() => {
        this.#due = false;
        const held = this.takeRecords();
        if (held.length === 0) return;
        this.#callback(new PerformanceObserverEntryList(recorded, held), this);
      });
    }
  }

  const performance = {
    get timeOrigin() {
      return started;
    },
    now() {
      return ops.op_zou_now();
    },
    mark(name, options) {
      return record(new PerformanceMark(name, options));
    },
    measure(name, startOrOptions, endMark) {
      let startTime;
      let endTime;
      let detail;
      const options = typeof startOrOptions === "object" && startOrOptions !== null ? startOrOptions : null;
      if (options) {
        if (endMark !== undefined) {
          throw new TypeError("a measure is given either an options object or an end mark, not both");
        }
        detail = options.detail;
        if (options.start !== undefined) startTime = whenWas(options.start);
        if (options.end !== undefined) endTime = whenWas(options.end);
        if (options.duration !== undefined) {
          const long = Number(options.duration);
          if (startTime !== undefined && endTime !== undefined) {
            throw new TypeError("a measure with a duration is given a start or an end, not both");
          }
          if (startTime === undefined && endTime === undefined) {
            throw new TypeError("a measure with a duration is given a start or an end");
          }
          if (startTime === undefined) startTime = endTime - long;
          else endTime = startTime + long;
        }
      } else {
        if (startOrOptions !== undefined) startTime = whenWas(startOrOptions);
        if (endMark !== undefined) endTime = whenWas(endMark);
      }
      // Missing ends mean the two the web fills in: nothing before the
      // isolate started, and now.
      if (startTime === undefined) startTime = 0;
      if (endTime === undefined) endTime = ops.op_zou_now();
      return record(new PerformanceMeasure(recorded, String(name), startTime, endTime - startTime, detail));
    },
    clearMarks(name) {
      forget("mark", name);
    },
    clearMeasures(name) {
      forget("measure", name);
    },
    getEntries() {
      return entries.slice();
    },
    getEntriesByName(name, type) {
      const wanted = String(name);
      return entries.filter(
        (entry) => entry.name === wanted && (type === undefined || entry.entryType === String(type)),
      );
    },
    getEntriesByType(type) {
      const wanted = String(type);
      return entries.filter((entry) => entry.entryType === wanted);
    },
    toJSON() {
      return { timeOrigin: started };
    },
  };

  /// Drop one kind of entry, either all of them or the ones under a
  /// name. In place, because the array is what every getter copies.
  function forget(kind, name) {
    const wanted = name === undefined ? undefined : String(name);
    for (let at = entries.length - 1; at >= 0; at -= 1) {
      const entry = entries[at];
      if (entry.entryType === kind && (wanted === undefined || entry.name === wanted)) {
        entries.splice(at, 1);
      }
    }
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

  /// Say that the loop is now waiting for a request.
  ///
  /// A module whose last line is `await app.listen({ port: 8000 })` is
  /// waiting here, and the host is waiting for that module to finish
  /// evaluating, and neither of those ends without being told. This is
  /// what tells it: the host stops waiting for the module and calls the
  /// entry point, which puts the request into the loop that parked.
  ///
  /// The promise it hands back is deliberately dropped. What is wanted
  /// is not its value but the wait itself being out there rather than
  /// in here, because a wait held in javascript is one the runtime
  /// underneath cannot see: a module halfway down its own top level
  /// with nothing outstanding is a deadlock as far as that runtime is
  /// concerned, and a request arriving with nothing outstanding does
  /// not wake anything up.
  ///
  /// Said again at every park rather than once, because a pooled
  /// isolate parks again after every call it answers.
  function parked() {
    ops.op_zou_parked();
  }

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
        return new Promise((resolve) => {
          accepting.accepts.push(resolve);
          parked();
        });
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
        return new Promise((resolve) => {
          accepting.nexts.push(resolve);
          parked();
        });
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
      this.ports = init.ports ?? [];
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

  /// The event a promise nobody caught is reported as. `promise` is the
  /// one that rejected and `reason` is what it rejected with, and
  /// calling `preventDefault` on it is how a listener says it has
  /// dealt with it and the runtime should not.
  class PromiseRejectionEvent extends Event {
    constructor(type, init = {}) {
      super(type, init);
      this.promise = init.promise ?? null;
      this.reason = init.reason;
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
        reported(thrown);
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
        reported(thrown);
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

  /// Report the exception, which is the html spec's name for what
  /// happens to a throw with nobody left above it to catch it: a timer
  /// callback, a microtask, an event listener. The web dispatches an
  /// `error` event on the global first and only writes it out if
  /// nobody prevented the default, so a function that wants to send
  /// its own crashes somewhere has a place to stand.
  ///
  /// Deno's answer to an unreported one is to end the process. Here it
  /// is written to stderr and the call goes on, because by the time
  /// one of these fires the answer is usually already written and
  /// losing it would be worse than the throw.
  let reporting = false;

  function reported(thrown) {
    // A listener for `error` that throws is reported the same way,
    // which without this is the same listener again and again. The one
    // that threw is written out and that is the end of it.
    if (reporting) {
      console.error(thrown);
      return;
    }
    const event = new ErrorEvent("error", {
      cancelable: true,
      message: String(thrown?.message ?? thrown),
      error: thrown,
    });
    reporting = true;
    let prevented = false;
    try {
      prevented = !fires(globalThis, event);
    } finally {
      reporting = false;
    }
    if (!prevented) {
      console.error(thrown);
    }
  }

  /// The events the host dispatches into a function about the
  /// function's own life, which is the one part of the event surface
  /// nothing inside the isolate can cause.
  ///
  /// `beforeunload` carries why on `detail.reason`, which is upstream's
  /// shape and one of `cpu`, `memory`, `wall_clock`, `early_drop` or
  /// `termination`. It is cancelable in the sense that the event says
  /// so, and preventing the default changes nothing: a limit reached is
  /// not a decision a function gets a vote on. What it is for is the
  /// last chance to write something down.
  function lifecycle(kind, reason) {
    switch (kind) {
      case "beforeunload":
        fires(
          globalThis,
          new CustomEvent("beforeunload", {
            cancelable: true,
            detail: { reason: reason ?? null },
          }),
        );
        return;
      default:
        fires(globalThis, new Event(kind));
    }
  }

  // ---------------------------------------------------------------
  // AbortController
  //
  // Here because the older way of serving needs it: `new Server(...)`
  // in `std/http/server.ts` makes a controller in a field initializer,
  // so a function written the way the older examples are written cannot
  // even be constructed without one.
  //
  // The three statics are here because a library reaching for a timeout
  // on a fetch is how a library bounds a call now. `jose` does it while
  // fetching a jwks, which is one line of somebody else's code deciding
  // whether a function answers at all.

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

    /// A signal that is already aborted, which is how a caller says no
    /// before the work is started.
    static abort(reason) {
      const signal = new AbortSignal(SIGNAL);
      signal[REASON] = reason === undefined ? aborted() : reason;
      return signal;
    }

    /// A signal that aborts itself after a while, with a
    /// `TimeoutError` rather than an `AbortError`, because a caller
    /// that gave up and a clock that ran out are two different things
    /// and a library tells them apart by the name.
    ///
    /// The timer is the isolate's, so a call that finished before the
    /// clock ran out leaves one pending. It fires into a signal
    /// nobody is listening to any more, which is a wasted wakeup and
    /// not a wasted answer.
    static timeout(ms) {
      const delay = Number(ms);
      if (!Number.isFinite(delay) || delay < 0) {
        throw new TypeError("AbortSignal.timeout takes a number of milliseconds");
      }
      const signal = new AbortSignal(SIGNAL);
      setTimeout(() => {
        if (!signal.aborted) {
          signal[REASON] = new DOMException("Signal timed out.", "TimeoutError");
          fires(signal, new Event("abort"));
        }
      }, delay);
      return signal;
    }

    /// One signal that follows whichever of these aborts first, which
    /// is how a caller's own signal and a timeout are handed to the
    /// same fetch.
    static any(signals) {
      const held = [...signals];
      for (const one of held) {
        if (!(one instanceof AbortSignal)) {
          throw new TypeError("AbortSignal.any takes AbortSignals");
        }
      }
      const signal = new AbortSignal(SIGNAL);
      const first = held.find((one) => one.aborted);
      if (first !== undefined) {
        signal[REASON] = first.reason;
        return signal;
      }
      for (const one of held) {
        one.addEventListener("abort", () => {
          if (!signal.aborted) {
            signal[REASON] = one.reason;
            fires(signal, new Event("abort"));
          }
        });
      }
      return signal;
    }
  }

  /// The default reason, which is what everything catching an abort is
  /// written against: `err.name === "AbortError"`.
  function aborted() {
    return new DOMException("The signal has been aborted", "AbortError");
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
      signal[REASON] = reason === undefined ? aborted() : reason;
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

  // What the function has written to its own environment, over what it
  // was started with.
  //
  // A write does not reach the host. The environment a node hands an
  // isolate is a project's secrets with the server's own names over
  // them, and none of that is a function's to change for anybody else.
  // What it can change is what it reads back, which is what a package
  // is doing when it sets `NODE_ENV` or its own key before it uses it,
  // and refusing that was stricter than both node and Deno for no gain
  // anybody could name. So the writes live here, in the isolate, and
  // they go when it does.
  //
  // A deletion is a name in `hidden` rather than a missing entry,
  // because what is underneath does not go away and a delete has to
  // cover it.
  const written = new Map();
  const hidden = new Set();

  const env = {
    get(name) {
      const key = String(name);
      if (hidden.has(key)) return undefined;
      if (written.has(key)) return written.get(key);
      const found = ops.op_zou_env_get(key);
      return found === null ? undefined : found;
    },
    has(name) {
      return env.get(name) !== undefined;
    },
    set(name, value) {
      const key = String(name);
      // The two characters that would make the name unreadable to
      // whoever parsed it back out, which is the check Deno makes.
      if (key.includes("=") || key.includes("\0")) {
        throw new TypeError(`the environment name ${JSON.stringify(key)} is not a name`);
      }
      const said = String(value);
      if (said.includes("\0")) {
        throw new TypeError(`the value of ${key} may not contain a null byte`);
      }
      hidden.delete(key);
      written.set(key, said);
    },
    delete(name) {
      const key = String(name);
      written.delete(key);
      hidden.add(key);
    },
    toObject() {
      const all = ops.op_zou_env();
      for (const key of hidden) delete all[key];
      for (const [key, value] of written) all[key] = value;
      return all;
    },
  };

  // ---------------------------------------------------------------
  // process
  //
  // A global rather than only a module, because that is what upstream's
  // runtime has: Deno puts `process` on the global and a package that
  // reads `process.env.NODE_ENV` at the top of a module gets an answer
  // there instead of a ReferenceError. `node:process` is this object
  // and not a second one, so a function that sets something on it sees
  // it whichever way it reached it.
  //
  // The version is the node a package should believe it is running on
  // when it branches on one, which is what the number is for: it is
  // checked far more often than it is printed.

  const environment = new Proxy(
    {},
    {
      get(_target, name) {
        return typeof name === "string" ? env.get(name) : undefined;
      },
      has(_target, name) {
        return typeof name === "string" && env.has(name);
      },
      set(_target, name, value) {
        if (typeof name !== "string") return false;
        env.set(name, value);
        return true;
      },
      deleteProperty(_target, name) {
        if (typeof name !== "string") return false;
        env.delete(name);
        return true;
      },
      ownKeys() {
        return Reflect.ownKeys(env.toObject());
      },
      getOwnPropertyDescriptor(_target, name) {
        const value = typeof name === "string" ? env.get(name) : undefined;
        return value === undefined
          ? undefined
          : { value, writable: true, enumerable: true, configurable: true };
      },
    },
  );

  /// A writer that goes to the console, because there is no file
  /// descriptor here to hand anybody. A package that writes a log line
  /// through `process.stdout` gets a log line.
  function writer(to) {
    let held = "";
    return {
      write(chunk) {
        held += typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk);
        // A line at a time, so a package writing "a" then "b\n" makes
        // one line rather than two.
        const lines = held.split("\n");
        held = lines.pop();
        for (const line of lines) {
          to(line);
        }
        return true;
      },
      end() {},
      on() {
        return this;
      },
      once() {
        return this;
      },
      removeListener() {
        return this;
      },
      isTTY: false,
      columns: 80,
      fd: to === console.error ? 2 : 1,
    };
  }

  const NODE = "20.11.1";

  const process = {
    env: environment,
    argv: ["node", "index"],
    argv0: "node",
    execPath: "/usr/local/bin/node",
    platform: "linux",
    arch: "x86_64",
    pid: 1,
    ppid: 0,
    title: "zou",
    version: `v${NODE}`,
    versions: { node: NODE, v8: "0.0.0" },
    release: { name: "node" },
    browser: false,
    exitCode: undefined,
    // Node runs these before promises rather than after, which nothing
    // here can offer: a microtask is the closest thing this event loop
    // has and the ordering difference has not been worth an op.
    nextTick(work, ...args) {
      queueMicrotask(() => work(...args));
    },
    cwd() {
      return "/";
    },
    chdir() {
      throw new TypeError("a function may not change the directory it runs in");
    },
    exit() {
      throw new TypeError("a function may not exit the process it is running in");
    },
    // The listener half is a no op that returns the object: nothing
    // here emits `exit` or `beforeExit`, and a package registering for
    // one should not crash for having asked.
    on() {
      return process;
    },
    once() {
      return process;
    },
    off() {
      return process;
    },
    addListener() {
      return process;
    },
    removeListener() {
      return process;
    },
    removeAllListeners() {
      return process;
    },
    emit() {
      return false;
    },
    listeners() {
      return [];
    },
    emitWarning(warning) {
      console.warn(warning);
    },
    uptime() {
      return (Date.now() - started) / 1000;
    },
    hrtime: Object.assign(
      function hrtime(since) {
        const now = performance.now() * 1e6;
        const nanoseconds = since ? now - (since[0] * 1e9 + since[1]) : now;
        return [Math.floor(nanoseconds / 1e9), Math.floor(nanoseconds % 1e9)];
      },
      {
        bigint() {
          return BigInt(Math.floor(performance.now() * 1e6));
        },
      },
    ),
    memoryUsage() {
      return { rss: 0, heapTotal: 0, heapUsed: 0, external: 0, arrayBuffers: 0 };
    },
    stdout: writer(console.log),
    stderr: writer(console.error),
    stdin: {
      read() {
        return null;
      },
      on() {
        return this;
      },
      setEncoding() {
        return this;
      },
      resume() {
        return this;
      },
      pause() {
        return this;
      },
      isTTY: false,
      fd: 0,
    },
    getuid() {
      return 0;
    },
    getgid() {
      return 0;
    },
    umask() {
      return 0o022;
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
  /// to serve, and the three a socket fails with.
  ///
  /// A read of a connection that was closed really does raise
  /// `BadResource` now, and a connection nobody is listening for really
  /// does raise `ConnectionRefused`, so these are the classes a library
  /// branches on rather than names kept for a path that never runs. The
  /// ones the server side would have raised are still that: there is no
  /// accept here to fail, and `error instanceof Deno.errors.BadResource`
  /// on an `undefined` is a TypeError.
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
  const ConnectionRefused = named("ConnectionRefused");
  const ConnectionAborted = named("ConnectionAborted");
  const TimedOut = named("TimedOut");

  /// A `string | URL`, which is what Deno takes, as the string the op
  /// wants. A file url is the path in it, since that is the only thing
  /// a file url is here.
  ///
  /// An http url is handed over whole, and the host reads it through
  /// the cache the modules are fetched into. A package here is a url
  /// rather than a directory, so a package's own file beside it is a
  /// url too, and `new URL('magick.wasm', import.meta.resolve('npm:...'))`
  /// is how a function asks for one.
  function pathOf(path) {
    if (path instanceof URL) {
      if (path.protocol === "https:" || path.protocol === "http:") {
        return path.href;
      }
      if (path.protocol !== "file:") {
        throw new TypeError(
          `a file may only be read through a file url or an http url, not ${path.protocol}`,
        );
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
  // Deno.connect
  //
  // A socket, which is what a database driver is written against: a
  // `Deno.connect`, then a `Deno.startTls` if the server says it
  // speaks TLS, then reads and writes of the wire protocol.
  //
  // `read` and `write` are the whole interface. `std/io`'s BufReader
  // and BufWriter are built on those two methods and nothing else, and
  // every driver in this corpus is built on those, so the streams
  // below are for the code that reaches for them rather than the way
  // the bytes usually move.
  //
  // A read copies once more than upstream's does. The op answers with
  // the bytes it got and this puts them into the buffer the caller
  // handed in, where upstream reads into that buffer directly. What
  // that buys is a runtime with no detached buffers in it, and what it
  // costs is a memcpy of at most sixty four kilobytes.

  const RID = Symbol("rid");
  const READABLE = Symbol("readable");
  const WRITABLE = Symbol("writable");

  /// The classes the host names a failure with, which are the ones a
  /// library catches by name.
  const FAILURES = {
    BadResource,
    BrokenPipe,
    ConnectionAborted,
    ConnectionRefused,
    ConnectionReset,
    Interrupted,
    InvalidData,
    NotConnected,
    NotFound,
    PermissionDenied,
    TimedOut,
    UnexpectedEof,
  };

  function failed(answer) {
    const Raised = FAILURES[answer.name] ?? Error;
    throw new Raised(answer.why);
  }

  class Conn {
    constructor(made) {
      this[RID] = made.rid;
      this[READABLE] = null;
      this[WRITABLE] = null;
      this.rid = made.rid;
      this.localAddr = made.local;
      this.remoteAddr = made.remote;
    }

    /// Bytes into the buffer that was handed in, and how many went
    /// into it, or `null` at the end of the stream.
    async read(buffer) {
      if (!ArrayBuffer.isView(buffer)) {
        throw new TypeError("read takes a buffer to read into");
      }
      const got = await ops.op_zou_tcp_read(this[RID], buffer.byteLength);
      if (got.kind === "failed") {
        failed(got);
      }
      if (got.kind === "eof") {
        return null;
      }
      new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength).set(got.bytes);
      return got.bytes.length;
    }

    /// Bytes out, and how many of them went, which may be fewer than
    /// were handed in: that is what a write is, and every caller of
    /// one loops.
    async write(bytes) {
      const wrote = await ops.op_zou_tcp_write(this[RID], bytesOf(bytes));
      if (wrote.kind === "failed") {
        failed(wrote);
      }
      return wrote.sent;
    }

    /// This end has nothing more to say, which is a half close and not
    /// a hang up: whatever the other end has still to send is still
    /// readable.
    closeWrite() {
      return ops.op_zou_tcp_shutdown(this[RID]);
    }

    close() {
      ops.op_zou_tcp_close(this[RID]);
    }

    get readable() {
      if (this[READABLE] === null) {
        const conn = this;
        this[READABLE] = new ReadableStream({
          async pull(controller) {
            const buffer = new Uint8Array(64 * 1024);
            const read = await conn.read(buffer);
            if (read === null) {
              controller.close();
              conn.close();
              return;
            }
            controller.enqueue(buffer.subarray(0, read));
          },
          cancel() {
            conn.close();
          },
        });
      }
      return this[READABLE];
    }

    get writable() {
      if (this[WRITABLE] === null) {
        const conn = this;
        this[WRITABLE] = new WritableStream({
          async write(chunk) {
            const bytes = bytesOf(chunk);
            let sent = 0;
            while (sent < bytes.byteLength) {
              sent += await conn.write(bytes.subarray(sent));
            }
          },
          close() {
            conn.close();
          },
          abort() {
            conn.close();
          },
        });
      }
      return this[WRITABLE];
    }

    /// Nagle is off on every socket this opens, which is what upstream
    /// does too, so asking for it again is asking for what is already
    /// true. Keep alive is the operating system's default.
    setNoDelay() {}
    setKeepAlive() {}
    ref() {}
    unref() {}
  }

  /// What was asked for, as a host and a port, with the transport this
  /// runtime will not open said by name.
  ///
  /// A unix socket is a file on the machine the function is running
  /// on rather than somewhere on the network, and a function here may
  /// not open the host's own files. That is the line, and it is the
  /// same line `Deno.readFile` draws.
  function connecting(options, what) {
    const transport = options.transport ?? "tcp";
    if (transport !== "tcp") {
      throw new TypeError(
        `${what} may only open a tcp connection, and this one asked for ${transport}`,
      );
    }
    const port = Number(options.port);
    if (!Number.isInteger(port) || port < 1 || port > 65535) {
      throw new TypeError(`${options.port} is not a port`);
    }
    return { hostname: options.hostname ?? "127.0.0.1", port };
  }

  function connOf(made) {
    if (made.kind === "failed") {
      failed(made);
    }
    return new Conn(made);
  }

  async function connect(options = {}) {
    const { hostname, port } = connecting(options, "a function");
    return connOf(await ops.op_zou_tcp_connect(hostname, port));
  }

  async function connectTls(options = {}) {
    const { hostname, port } = connecting(options, "a function");
    return connOf(await ops.op_zou_tcp_connect_tls(hostname, port, options.caCerts ?? []));
  }

  /// TLS on a connection that is already open, which is how postgres
  /// and every STARTTLS protocol does it: ask in the clear whether the
  /// server speaks it, then speak it.
  ///
  /// The connection handed in is gone afterwards and what comes back is
  /// a new one, which is upstream's shape as well.
  async function startTls(conn, options = {}) {
    if (!(conn instanceof Conn)) {
      throw new TypeError("startTls takes a connection this runtime opened");
    }
    const hostname = options.hostname ?? conn.remoteAddr?.hostname ?? "127.0.0.1";
    const made = await ops.op_zou_tcp_start_tls(conn[RID], hostname, options.caCerts ?? []);
    return connOf(made);
  }

  // ---------------------------------------------------------------
  // Deno.resolveDns, and the address lookup underneath node:dns
  //
  // Two different things that both look like asking about a name. A
  // lookup is the host's own resolution, the one every other program on
  // the machine gets, so `localhost`, a line in `/etc/hosts` and a
  // search domain all mean what they mean everywhere else. A resolve is
  // a query put on the wire for records of one type, which is the only
  // way to see an MX or a TXT at all.
  //
  // The lookup is not a Deno api because there is no Deno api for it,
  // and inventing a name on `Deno` for something upstream does not have
  // would be a function written against this runtime and no other. It
  // is an internal instead, where `node:dns` can reach it and a
  // function has no reason to.

  const RECORDS = ["A", "AAAA", "CAA", "CNAME", "MX", "NS", "PTR", "SOA", "SRV", "TXT"];

  /// A failure with the code node's dns errors carry, since a package
  /// that catches one branches on the code rather than on the message.
  function unresolved(answer) {
    const Raised = answer.code === "ENOTFOUND" || answer.code === "ENODATA" ? NotFound : Error;
    const wrong = new Raised(answer.why);
    wrong.code = answer.code;
    throw wrong;
  }

  async function resolveDns(query, recordType, options = {}) {
    const kind = String(recordType ?? "").toUpperCase();
    if (!RECORDS.includes(kind)) {
      throw new TypeError(
        `${recordType} is not a record type this runtime asks for, which are ${RECORDS.join(", ")}`,
      );
    }
    const named = options?.nameServer;
    // A v6 address has colons of its own, so it is the port that has to
    // be told apart rather than the address.
    const at = named?.ipAddr?.includes(":") && !named.ipAddr.startsWith("[")
      ? `[${named.ipAddr}]`
      : named?.ipAddr;
    const server = named === undefined || named === null ? "" : `${at}:${named.port ?? 53}`;
    const found = await ops.op_zou_dns_resolve(String(query), kind, server);
    if (found.kind === "failed") {
      unresolved(found);
    }
    return found.records;
  }

  globalThis.__zouLookup = async function lookup(hostname) {
    const looked = await ops.op_zou_dns_lookup(String(hostname));
    if (looked.kind === "failed") {
      unresolved(looked);
    }
    return looked.addresses;
  };

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
  // Copying a value

  // A deep copy a library can rely on, which is not a spread and is not
  // a trip through JSON: a cycle stays a cycle, a value that appears
  // twice arrives as one object twice, and a `Map`, a `Set`, a `Date`,
  // a `RegExp`, a buffer, a typed array and a `BigInt` arrive as
  // themselves rather than as whatever JSON would have made of them.
  //
  // The copy is v8's own serializer, which is what upstream's is too,
  // so the shapes it carries and the sentence it refuses with are v8's
  // rather than this file's, and a function or a symbol is refused with
  // the same words on both servers.
  //
  // What does not survive is a platform object. A `Blob`, a `Headers`,
  // a `URL`, a `Response` and a stream keep what they hold under
  // symbols, and the serializer copies own string keyed properties, so
  // a copy of one is an empty object rather than a copy or a refusal.
  // That is measured on a real `supabase start` rather than decided
  // here, and it is written down in `docs/functions.md`, because a
  // library cloning options with a `Blob` in them loses it on both
  // servers and finding that out here is cheaper than finding it out
  // there.
  function structuredClone(value, options) {
    if (arguments.length < 1) {
      throw new TypeError(
        "Failed to execute 'structuredClone': 1 argument required, but only 0 present",
      );
    }
    return copied(value, checkTransfer(options)).data;
  }

  // The copy itself, shared with `postMessage` rather than written
  // twice, so a message and a clone carry the same shapes and refuse
  // the same values with the same words.
  //
  // A `MessagePort` is the one thing here v8 cannot copy and this file
  // can move. Every port carries v8's host object brand, so the
  // serializer stops and asks about it rather than writing it out as
  // the empty object its own string keyed properties would make: a
  // port that was named for transfer is written as its place in the
  // list and read back as the fresh port that took its end over, and a
  // port that was not is refused in v8's words, `Unsupported object
  // type`, which is the same refusal upstream gives.
  function copied(value, list) {
    const ports = list.filter((item) => item instanceof MessagePort);
    const bytes = core.serialize(
      value,
      ports.length > 0 ? { hostObjects: ports } : undefined,
      (message) => {
        throw new DOMException(message, "DataCloneError");
      },
    );
    // After the writing rather than before it, because a value that
    // cannot be copied throws and a throw leaves the ports where they
    // were.
    const arrived = ports.map(handedOver);
    return {
      data: core.deserialize(
        bytes,
        arrived.length > 0 ? { hostObjects: arrived } : undefined,
      ),
      ports: arrived,
    };
  }

  // The transfer list, read and checked and then acted on for a port
  // and not for a buffer.
  //
  // The buffer is upstream rather than an omission: one named for
  // transfer is copied like everything else and is still readable
  // afterwards, measured by cloning one and asking its `byteLength`,
  // where a browser and a newer Deno both leave it detached. The
  // checking is not idle, because these three refusals are the whole of
  // what a caller passing the wrong thing sees, and a library that
  // transfers a buffer it then reuses works here and works there.
  function checkTransfer(options, called = "structuredClone") {
    if (options === undefined || options === null) return [];
    if (typeof options !== "object" && typeof options !== "function") {
      throw new TypeError(
        `Failed to execute '${called}': Argument 2 can not be converted to a dictionary`,
      );
    }
    const list = options.transfer;
    if (list === undefined) return [];
    if (
      list === null ||
      typeof list !== "object" ||
      typeof list[Symbol.iterator] !== "function"
    ) {
      throw new TypeError(
        `Failed to execute '${called}': 'transfer' of 'StructuredSerializeOptions' ` +
          "(Argument 2) can not be converted to sequence.",
      );
    }
    const named = [];
    let index = 0;
    for (const item of list) {
      if (item === null || (typeof item !== "object" && typeof item !== "function")) {
        throw new TypeError(
          `Failed to execute '${called}': 'transfer' of 'StructuredSerializeOptions' ` +
            `(Argument 2), index ${index} is not an object`,
        );
      }
      // An `ArrayBuffer` and a `MessagePort` are the transferable
      // things here, and only the port is really moved. A stream and a
      // typed array are both refused upstream.
      if (!(item instanceof ArrayBuffer) && !(item instanceof MessagePort)) {
        throw new DOMException("Value not transferable", "DataCloneError");
      }
      named.push(item);
      index += 1;
    }
    return named;
  }

  // ---------------------------------------------------------------
  // A channel with two ends

  // Two ports, each holding the other's end, where what is posted into
  // one arrives at the other as an event. There is one isolate here and
  // no worker to be on the far side, so both ends are in this call, and
  // the point of them is not the thread they cross but the queue they
  // are: a library that wants a reader on one side and a writer on the
  // other, an sdk that hands work to itself and waits, a wrapper that
  // adapts a callback into an async iterator. Several of them make a
  // channel whether or not they ever send anything across it, and until
  // now that was a `ReferenceError` at import time.
  //
  // What arrives is a copy taken when it was posted, not the object,
  // and it goes through the same serializer `structuredClone` uses.
  //
  // The end, which is what actually holds the channel: what has
  // arrived and not been read yet, whether anybody has started
  // reading, and the other end. It outlives the port it is attached to,
  // because transferring a port means giving its end to a new one, and
  // what it was holding goes with it.
  const END = Symbol("end");
  const ONMESSAGE = Symbol("onmessage");
  const ONMESSAGEERROR = Symbol("onmessageerror");
  const PORT1 = Symbol("port1");
  const PORT2 = Symbol("port2");

  // v8's own brand for an object the host owns. The serializer asks
  // about anything carrying it rather than writing out its properties,
  // which is what makes a port either a transfer or a refusal instead
  // of quietly arriving as `{}`.
  const HOST = Symbol.for("Deno.core.hostObject");

  const arriving = [];
  let scheduled = false;

  // When a message is delivered, which is the part a library can tell
  // apart from the outside. A message posted from a call is delivered
  // on the microtask queue, so it arrives ahead of a timer that was set
  // before it, which is what the reference runtime does and what a race
  // between a message and a `setTimeout` deadline is written against.
  function queued(end) {
    if (!arriving.includes(end)) {
      arriving.push(end);
    }
    if (scheduled) {
      return;
    }
    scheduled = true;
    Promise.resolve().then(delivers);
  }

  // A round of messages is what was waiting when the round began, and
  // what a handler posts while it is running belongs to the next one.
  // That next round is booked before any handler runs, which is what
  // puts it ahead of a timer a handler sets and behind a timer that was
  // already waiting, and it is why two ports answering each other never
  // starve the timers. The reference runtime interleaves them the same
  // way, one round of messages per turn of the loop.
  function delivers() {
    scheduled = false;
    const ends = arriving.splice(0, arriving.length);
    if (ends.length === 0) {
      return;
    }
    scheduled = true;
    after(delivers, 0, [], false);
    for (const end of ends) {
      let left = end.holding.length;
      while (left > 0 && end.started && !end.closed) {
        const message = end.holding.shift();
        left -= 1;
        fires(
          end.port,
          new MessageEvent("message", { data: message.data, ports: message.ports }),
        );
      }
      if (end.holding.length > 0 && end.started && !end.closed) {
        queued(end);
      }
    }
  }

  // A port is never constructed by a caller: it comes from a channel or
  // from a transfer, and `new MessagePort()` is the same refusal
  // upstream gives.
  let entangling = false;

  function made() {
    entangling = true;
    try {
      return new MessagePort();
    } finally {
      entangling = false;
    }
  }

  // Transferring a port: the end moves to a fresh port, listeners and
  // all the rest of the old one stay behind, and the old one is left
  // holding nothing. Posting into it afterwards is not an error, it
  // just reaches nobody, which is measured rather than chosen.
  function handedOver(port) {
    const end = port[END];
    const fresh = made();
    port[END] = null;
    if (end !== null) {
      end.port = fresh;
      fresh[END] = end;
    }
    return fresh;
  }

  // The transfer list of a `postMessage`, which takes either the array
  // itself or an options object with it under `transfer`.
  function transferred(transfer, from) {
    let list = transfer;
    if (list === null) {
      list = undefined;
    } else if (list !== undefined && !Array.isArray(list) && typeof list === "object") {
      list = list.transfer;
    }
    const named = checkTransfer({ transfer: list }, "postMessage");
    for (const item of named) {
      if (item === from) {
        throw new DOMException("Can not transfer self", "DataCloneError");
      }
    }
    return named;
  }

  class MessagePort extends EventTarget {
    constructor() {
      if (!entangling) {
        throw new TypeError("Illegal constructor");
      }
      super();
      this[END] = null;
      this[ONMESSAGE] = null;
      this[ONMESSAGEERROR] = null;
    }

    get onmessage() {
      return this[ONMESSAGE];
    }

    // Setting a handler starts the port, which is the difference
    // between this and `addEventListener`: a handler set long after the
    // messages were posted still sees them, and a listener added the
    // other way sees nothing until somebody calls `start`. Setting it
    // to null does not start anything, and does not stop a port that
    // was already started either.
    set onmessage(handler) {
      this[ONMESSAGE] = handler;
      if (handler !== null && handler !== undefined) {
        this.start();
      }
    }

    get onmessageerror() {
      return this[ONMESSAGEERROR];
    }

    set onmessageerror(handler) {
      this[ONMESSAGEERROR] = handler;
    }

    postMessage(data, transfer) {
      // The copy is made before the port is looked at, so a value that
      // cannot be copied is refused whether or not anybody is left to
      // receive it.
      const message = copied(data, transferred(transfer, this));
      const end = this[END];
      const other = end === null ? null : end.other;
      if (other === null || other.closed) {
        return;
      }
      other.holding.push(message);
      if (other.started) {
        queued(other);
      }
    }

    start() {
      const end = this[END];
      if (end === null || end.started || end.closed) {
        return;
      }
      end.started = true;
      if (end.holding.length > 0) {
        queued(end);
      }
    }

    // Closing throws away what has arrived and not been read, and takes
    // the other end with it: a message posted after this reaches
    // nobody, and one posted before it and not yet delivered is gone.
    close() {
      const end = this[END];
      if (end === null || end.closed) {
        return;
      }
      end.closed = true;
      end.holding.length = 0;
      const other = end.other;
      end.other = null;
      if (other !== null) {
        other.other = null;
      }
    }
  }

  MessagePort.prototype[HOST] = true;
  MessagePort.prototype[Symbol.toStringTag] = "MessagePort";

  class MessageChannel {
    constructor() {
      const one = made();
      const two = made();
      const here = { port: one, other: null, holding: [], started: false, closed: false };
      const there = { port: two, other: here, holding: [], started: false, closed: false };
      here.other = there;
      one[END] = here;
      two[END] = there;
      this[PORT1] = one;
      this[PORT2] = two;
    }

    get port1() {
      return this[PORT1];
    }

    get port2() {
      return this[PORT2];
    }
  }

  MessageChannel.prototype[Symbol.toStringTag] = "MessageChannel";

  // ---------------------------------------------------------------
  // require, which is how an npm package that is still a script runs

  // Every script this call has run, by the url it was read from, so a
  // package required twice is one object and a package that requires
  // itself in a circle gets what has been set so far rather than a
  // second run. Node's module cache, and the same rule: a script runs
  // once and its exports are shared.
  const scripts = new Map();

  // A script asks for a built in by name, and the answer has to be a
  // value already, because a script cannot wait. The module that stands
  // in for the script imports them all before it starts, which is what
  // leaves them here.
  function builtin(name) {
    const held = globalThis.__zouBuiltins;
    const found = held === undefined ? undefined : held.get(name);
    if (found === undefined) {
      throw new Error(`${name} is a node built in this runtime does not have`);
    }
    return found;
  }

  // The path a file url names, since a script is handed `__filename`
  // and works `__dirname` out of it, and neither is a url in node.
  function dirOf(path) {
    const cut = path.lastIndexOf("/");
    return cut <= 0 ? "/" : path.slice(0, cut);
  }

  function ran(url) {
    const already = scripts.get(url);
    if (already !== undefined) {
      return already;
    }
    const script = ops.op_zou_cjs_read(url);
    // In the map before it runs, so that a script requiring something
    // that requires it back gets the half built exports object instead
    // of running a second copy of it. That is node's answer to a cycle
    // and packages are written against it.
    const module = { id: script.path, filename: script.path, exports: {}, loaded: false };
    scripts.set(url, module);
    if (script.data) {
      module.exports = JSON.parse(script.text);
      module.loaded = true;
      return module;
    }
    const require = (spec) => {
      const to = ops.op_zou_cjs_resolve(String(spec), url);
      return to.startsWith("node:") ? builtin(to) : ran(to).exports;
    };
    // The two of these that packages actually call. `resolve` answers
    // with a path because that is what node answers with, and a script
    // that has one usually reads a file beside it.
    require.resolve = (spec) => {
      const to = ops.op_zou_cjs_resolve(String(spec), url);
      return to.startsWith("node:") ? to : ops.op_zou_cjs_read(to).path;
    };
    require.cache = scripts;
    // A `new Function` rather than an eval, so the script's own
    // variables stay inside it and the five names node gives a module
    // are the five it can see. The newline is for a file that ends in a
    // comment with no line break after it.
    //
    // `global` is the sixth, and it is here rather than on the global
    // object because that is where node has it and where Deno has it:
    // node's `global` is the global object under its other name, and a
    // package that ships one build for node and one for a browser
    // decides which of the two it is by asking whether the name exists.
    // A function's own module is not node code and does not get it; a
    // script out of a package is, and does.
    const run = new Function(
      "exports",
      "require",
      "module",
      "__filename",
      "__dirname",
      "global",
      `${script.text}\n`,
    );
    run.call(
      module.exports,
      module.exports,
      require,
      module,
      script.path,
      dirOf(script.path),
      globalThis,
    );
    module.loaded = true;
    return module;
  }

  // What the module standing in for a script calls. It is on the global
  // because the module is text this runtime generated and there is
  // nowhere else for generated text to look, which is also why the name
  // says whose it is.
  globalThis.__zouRequire = ran;

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
    MessageChannel,
    MessageEvent,
    MessagePort,
    PerformanceEntry,
    PerformanceMark,
    PerformanceMeasure,
    PerformanceObserver,
    PerformanceObserverEntryList,
    PromiseRejectionEvent,
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
    process,
    queueMicrotask,
    setInterval,
    setTimeout,
    structuredClone,
    // The two node names for a timer of no length, which upstream's
    // runtime has on the global as well. A package built for node
    // calls one of them at the bottom of a promise chain often enough
    // that being without them is a ReferenceError in library code.
    setImmediate: (work, ...args) => setTimeout(() => work(...args), 0),
    clearImmediate: clearTimer,
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

  // A promise that rejected with nobody to catch it. deno_core asks
  // this before it does the thing it does otherwise, which is to raise
  // an error nothing can catch and stop the isolate, and answering
  // true is saying the rejection has been dealt with.
  //
  // It is dealt with when a listener says so by preventing the
  // default, which is the web's rule and upstream's. Otherwise it is
  // written out here and the call is allowed to go on: a rejected
  // promise in some library's retry loop is not a reason to lose an
  // answer that is already written.
  core.setUnhandledPromiseRejectionHandler((promise, reason) => {
    const event = new PromiseRejectionEvent("unhandledrejection", {
      cancelable: true,
      promise,
      reason,
    });
    if (!fires(globalThis, event)) {
      return true;
    }
    console.error("uncaught promise rejection:", reason);
    return true;
  });

  // The other side of it: a promise that was rejected long enough for
  // the runtime to have given up on it and then caught after all.
  core.setHandledPromiseRejectionHandler((promise, reason) => {
    fires(globalThis, new PromiseRejectionEvent("rejectionhandled", { promise, reason }));
  });

  // What deno_core does with a throw out of a microtask, which is the
  // one report path it owns rather than the prelude.
  core.setReportExceptionCallback(reported);

  Object.assign(Deno, {
    serve,
    env,
    readFile,
    readFileSync,
    readTextFile,
    readTextFileSync,
    listen,
    serveHttp,
    connect,
    connectTls,
    startTls,
    resolveDns,
    // The two a function catching one of them by name is written
    // against, which is what makes a missing file and a file it may
    // not have two different things to it, the eight the older way of
    // serving names in a catch or throws itself, and the three a
    // socket fails with.
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
      ConnectionRefused,
      ConnectionAborted,
      TimedOut,
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

  // Three of them: the call, whatever the call left running, and the
  // events the host tells the function about its own life. The host
  // calls the first, takes the answer, and only then calls the second,
  // which is the whole of what `waitUntil` means. The third it calls
  // whenever it has something to say.
  return [run, drain, lifecycle];
})(globalThis);
