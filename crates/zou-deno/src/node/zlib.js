// node:zlib, which is gzip, deflate and their inflates in the three
// shapes node offers each of them in: a synchronous call, a call with
// a callback, and a stream.
//
// The compression itself is not here. It is the deflate this runtime
// already links in to open a package tarball, reached through an id a
// job is held by, so a stream can hand it four kilobytes at a time and
// get a single gzip out at the end rather than a hundred of them
// stapled together.
//
// What is missing is missing by name. Brotli is a different algorithm
// and not another header on this one, so `brotliCompress` and the rest
// of that family refuse rather than pretending, and so do the two
// dictionary settings, which zlib takes and this does not pass on.

import { Buffer } from "node:buffer";
import { Transform } from "node:stream";

const ops = Deno.core.ops;

// Node's numbers, which a package passes back to node and never reads.
// They are here because an options object built from them should not
// be full of `undefined`.
export const constants = {
  Z_NO_FLUSH: 0,
  Z_PARTIAL_FLUSH: 1,
  Z_SYNC_FLUSH: 2,
  Z_FULL_FLUSH: 3,
  Z_FINISH: 4,
  Z_BLOCK: 5,
  Z_OK: 0,
  Z_STREAM_END: 1,
  Z_NEED_DICT: 2,
  Z_ERRNO: -1,
  Z_STREAM_ERROR: -2,
  Z_DATA_ERROR: -3,
  Z_MEM_ERROR: -4,
  Z_BUF_ERROR: -5,
  Z_VERSION_ERROR: -6,
  Z_NO_COMPRESSION: 0,
  Z_BEST_SPEED: 1,
  Z_BEST_COMPRESSION: 9,
  Z_DEFAULT_COMPRESSION: -1,
  Z_DEFAULT_STRATEGY: 0,
  Z_FILTERED: 1,
  Z_HUFFMAN_ONLY: 2,
  Z_RLE: 3,
  Z_FIXED: 4,
  DEFLATE: 1,
  INFLATE: 2,
  GZIP: 3,
  GUNZIP: 4,
  DEFLATERAW: 5,
  INFLATERAW: 6,
  UNZIP: 7,
};

/// The bytes a caller handed in, whatever they handed them in as. A
/// string is utf8, which is what node reads one as here.
function bytesOf(input) {
  if (typeof input === "string") return new TextEncoder().encode(input);
  if (input instanceof ArrayBuffer) return new Uint8Array(input);
  if (ArrayBuffer.isView(input)) {
    return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  }
  const wrong = new TypeError(
    "The \"buffer\" argument must be of type string or an instance of Buffer, TypedArray, DataView, or ArrayBuffer",
  );
  wrong.code = "ERR_INVALID_ARG_TYPE";
  throw wrong;
}

/// The level an options object asks for, in the one number the op
/// takes. Node's default is the library's default and not nine.
function levelOf(options) {
  const asked = options === null || options === undefined ? undefined : options.level;
  if (asked === undefined || asked === null) return -1;
  const level = Number(asked);
  if (!Number.isInteger(level) || level < -1 || level > 9) {
    const wrong = new RangeError(`The value of "options.level" is out of range. Received ${asked}`);
    wrong.code = "ERR_OUT_OF_RANGE";
    throw wrong;
  }
  return level;
}

/// A failure from the library, in the shape node throws one: a code a
/// caller branches on, and an errno it almost never reads.
function failed(why) {
  const wrong = why instanceof Error ? why : new Error(String(why));
  wrong.code = wrong.code ?? "Z_DATA_ERROR";
  wrong.errno = wrong.errno ?? constants.Z_DATA_ERROR;
  return wrong;
}

function joined(parts) {
  let size = 0;
  for (const part of parts) size += part.length;
  const all = new Uint8Array(size);
  let at = 0;
  for (const part of parts) {
    all.set(part, at);
    at += part.length;
  }
  return Buffer.from(all.buffer, all.byteOffset, all.byteLength);
}

/// One shot: open, write everything, end. This is what every `*Sync`
/// call is, and what the callback ones do before calling back.
function once(kind, input, options) {
  const bytes = bytesOf(input);
  const id = ops.op_zou_zlib_open(kind, levelOf(options));
  try {
    const first = ops.op_zou_zlib_write(id, bytes);
    const last = ops.op_zou_zlib_end(id);
    return joined([first, last]);
  } catch (why) {
    ops.op_zou_zlib_drop(id);
    throw failed(why);
  }
}

/// A transform that is a compression, which is the whole of what every
/// `create*` returns. The job is opened when the first chunk arrives
/// rather than at construction, so a stream nobody writes to and
/// nobody ends does not hold one.
class Zlib extends Transform {
  #kind;
  #level;
  #id = 0;

  constructor(kind, options) {
    super(options);
    this.#kind = kind;
    this.#level = levelOf(options);
    this.bytesWritten = 0;
  }

  #open() {
    if (this.#id === 0) this.#id = ops.op_zou_zlib_open(this.#kind, this.#level);
    return this.#id;
  }

  _transform(chunk, encoding, back) {
    try {
      const bytes = bytesOf(chunk);
      this.bytesWritten += bytes.length;
      const made = ops.op_zou_zlib_write(this.#open(), bytes);
      back(null, made.length > 0 ? Buffer.from(made) : undefined);
    } catch (why) {
      this.#drop();
      back(failed(why));
    }
  }

  _flush(back) {
    try {
      const made = ops.op_zou_zlib_end(this.#open());
      this.#id = 0;
      back(null, made.length > 0 ? Buffer.from(made) : undefined);
    } catch (why) {
      this.#drop();
      back(failed(why));
    }
  }

  #drop() {
    if (this.#id !== 0) {
      ops.op_zou_zlib_drop(this.#id);
      this.#id = 0;
    }
  }

  // Node's own name for it, and the one `pipeline` calls on the way
  // out when something upstream failed.
  destroy(why) {
    this.#drop();
    return super.destroy ? super.destroy(why) : this;
  }

  close(back) {
    this.#drop();
    if (typeof back === "function") queueMicrotask(back);
  }
}

/// The callback form, whose work is synchronous and whose answer is
/// not: node calls back from the thread pool, and a package that reads
/// a variable the callback sets on the next line is broken either way.
function later(kind, input, options, back) {
  if (typeof options === "function") {
    back = options;
    options = {};
  }
  if (typeof back !== "function") {
    const wrong = new TypeError("The \"callback\" argument must be of type function");
    wrong.code = "ERR_INVALID_ARG_TYPE";
    throw wrong;
  }
  let made;
  try {
    made = once(kind, input, options);
  } catch (why) {
    queueMicrotask(() => back(why));
    return;
  }
  queueMicrotask(() => back(null, made));
}

export function gzipSync(input, options) {
  return once("gzip", input, options);
}
export function gunzipSync(input, options) {
  return once("gunzip", input, options);
}
export function deflateSync(input, options) {
  return once("deflate", input, options);
}
export function inflateSync(input, options) {
  return once("inflate", input, options);
}
export function deflateRawSync(input, options) {
  return once("deflateRaw", input, options);
}
export function inflateRawSync(input, options) {
  return once("inflateRaw", input, options);
}
export function unzipSync(input, options) {
  return once("unzip", input, options);
}

export function gzip(input, options, back) {
  later("gzip", input, options, back);
}
export function gunzip(input, options, back) {
  later("gunzip", input, options, back);
}
export function deflate(input, options, back) {
  later("deflate", input, options, back);
}
export function inflate(input, options, back) {
  later("inflate", input, options, back);
}
export function deflateRaw(input, options, back) {
  later("deflateRaw", input, options, back);
}
export function inflateRaw(input, options, back) {
  later("inflateRaw", input, options, back);
}
export function unzip(input, options, back) {
  later("unzip", input, options, back);
}

export function createGzip(options) {
  return new Zlib("gzip", options);
}
export function createGunzip(options) {
  return new Zlib("gunzip", options);
}
export function createDeflate(options) {
  return new Zlib("deflate", options);
}
export function createInflate(options) {
  return new Zlib("inflate", options);
}
export function createDeflateRaw(options) {
  return new Zlib("deflateRaw", options);
}
export function createInflateRaw(options) {
  return new Zlib("inflateRaw", options);
}
export function createUnzip(options) {
  return new Zlib("unzip", options);
}

/// The classes node exports beside the factories. A caller doing
/// `new zlib.Gzip()` gets the same object `createGzip()` returns.
export class Gzip extends Zlib {
  constructor(options) {
    super("gzip", options);
  }
}
export class Gunzip extends Zlib {
  constructor(options) {
    super("gunzip", options);
  }
}
export class Deflate extends Zlib {
  constructor(options) {
    super("deflate", options);
  }
}
export class Inflate extends Zlib {
  constructor(options) {
    super("inflate", options);
  }
}
export class DeflateRaw extends Zlib {
  constructor(options) {
    super("deflateRaw", options);
  }
}
export class InflateRaw extends Zlib {
  constructor(options) {
    super("inflateRaw", options);
  }
}
export class Unzip extends Zlib {
  constructor(options) {
    super("unzip", options);
  }
}

/// Brotli, which this runtime does not have. Every one of these is a
/// refusal by name, because a package that finds `brotliCompress`
/// defined will use it.
function noBrotli(name) {
  return function () {
    throw new Error(`node:zlib ${name} is brotli, which this runtime does not have`);
  };
}

export const brotliCompress = noBrotli("brotliCompress");
export const brotliCompressSync = noBrotli("brotliCompressSync");
export const brotliDecompress = noBrotli("brotliDecompress");
export const brotliDecompressSync = noBrotli("brotliDecompressSync");
export const createBrotliCompress = noBrotli("createBrotliCompress");
export const createBrotliDecompress = noBrotli("createBrotliDecompress");

/// Node's `crc32`, which is the one thing in this module that is not a
/// compression: a checksum over bytes, seeded by whatever the caller
/// had already. It is here because it is cheap and because a package
/// writing its own zip file asks for it.
export function crc32(input, value = 0) {
  const bytes = bytesOf(input);
  let crc = (~value) >>> 0;
  for (let at = 0; at < bytes.length; at++) {
    crc = (crc >>> 8) ^ TABLE[(crc ^ bytes[at]) & 0xff];
  }
  return (~crc) >>> 0;
}

const TABLE = (() => {
  const table = new Uint32Array(256);
  for (let at = 0; at < 256; at++) {
    let value = at;
    for (let bit = 0; bit < 8; bit++) {
      value = value & 1 ? 0xedb88320 ^ (value >>> 1) : value >>> 1;
    }
    table[at] = value >>> 0;
  }
  return table;
})();

// Node hangs the `Z_` numbers off the module as well as off
// `constants`, and a package written before `constants` existed reads
// them there, so the default export carries both.
export default {
  ...constants,
  constants,
  crc32,
  gzip,
  gzipSync,
  gunzip,
  gunzipSync,
  deflate,
  deflateSync,
  inflate,
  inflateSync,
  deflateRaw,
  deflateRawSync,
  inflateRaw,
  inflateRawSync,
  unzip,
  unzipSync,
  createGzip,
  createGunzip,
  createDeflate,
  createInflate,
  createDeflateRaw,
  createInflateRaw,
  createUnzip,
  Gzip,
  Gunzip,
  Deflate,
  Inflate,
  DeflateRaw,
  InflateRaw,
  Unzip,
  brotliCompress,
  brotliCompressSync,
  brotliDecompress,
  brotliDecompressSync,
  createBrotliCompress,
  createBrotliDecompress,
};
