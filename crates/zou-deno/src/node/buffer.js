// node:buffer. A Buffer is a Uint8Array with the encodings node reads
// and writes on it, which is what it is in node too: the class extends
// the typed array there, and every package that takes a Buffer and
// hands it to something web shaped depends on that being true.
//
// What is here is the constructors, the encodings, the comparisons and
// the fixed width reads and writes. What is not is the pool: node's
// `allocUnsafe` hands out slices of a shared arena and this one
// allocates, which costs an allocation and cannot ever hand a caller
// somebody else's bytes.

const encoder = new TextEncoder();
const decoders = new Map();

function decoderFor(encoding) {
  let held = decoders.get(encoding);
  if (held === undefined) {
    held = new TextDecoder(encoding);
    decoders.set(encoding, held);
  }
  return held;
}

const ENCODINGS = new Set([
  "utf8",
  "utf-8",
  "hex",
  "base64",
  "base64url",
  "latin1",
  "binary",
  "ascii",
  "ucs2",
  "ucs-2",
  "utf16le",
  "utf-16le",
]);

function named(encoding) {
  const name = String(encoding ?? "utf8").toLowerCase();
  if (!ENCODINGS.has(name)) {
    throw new TypeError(`Unknown encoding: ${encoding}`);
  }
  return name;
}

class Buffer extends Uint8Array {
  static poolSize = 8192;

  static from(value, a, b) {
    if (typeof value === "string") {
      return fromString(value, a);
    }
    if (value instanceof ArrayBuffer) {
      const view = new Uint8Array(value, a ?? 0, b ?? value.byteLength - (a ?? 0));
      // A view onto the same memory and not a copy, which is what node
      // does and what a caller handing over an ArrayBuffer expects.
      return asBuffer(view);
    }
    if (ArrayBuffer.isView(value)) {
      return copied(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
    }
    if (Array.isArray(value)) {
      return copied(Uint8Array.from(value, (it) => Number(it) & 0xff));
    }
    if (value !== null && typeof value === "object" && Array.isArray(value.data)) {
      // What `buffer.toJSON()` gives back, which is how a Buffer
      // survives a round trip through JSON.
      return copied(Uint8Array.from(value.data, (it) => Number(it) & 0xff));
    }
    if (value !== null && typeof value === "object" && typeof value[Symbol.iterator] === "function") {
      return copied(Uint8Array.from(value, (it) => Number(it) & 0xff));
    }
    throw new TypeError(
      "The first argument must be of type string or an instance of Buffer, ArrayBuffer, or Array",
    );
  }

  static of(...bytes) {
    return copied(Uint8Array.from(bytes));
  }

  static alloc(size, fill, encoding) {
    const made = new Buffer(size);
    if (fill !== undefined && fill !== 0) {
      made.fill(fill, 0, size, encoding);
    }
    return made;
  }

  /// The same as `alloc` here. Node hands back memory it has not
  /// cleared, which is faster and is how one request's bytes end up in
  /// another request's buffer when a package forgets to write all of
  /// it. This runtime does not have that failure mode.
  static allocUnsafe(size) {
    return new Buffer(size);
  }

  static allocUnsafeSlow(size) {
    return new Buffer(size);
  }

  static isBuffer(value) {
    return value instanceof Buffer;
  }

  static isEncoding(encoding) {
    return typeof encoding === "string" && ENCODINGS.has(encoding.toLowerCase());
  }

  static byteLength(value, encoding) {
    if (typeof value === "string") {
      return fromString(value, encoding).length;
    }
    if (ArrayBuffer.isView(value)) {
      return value.byteLength;
    }
    if (value instanceof ArrayBuffer) {
      return value.byteLength;
    }
    throw new TypeError("The first argument must be of type string or an ArrayBuffer view");
  }

  static concat(list, total) {
    if (!Array.isArray(list)) {
      throw new TypeError('The "list" argument must be an instance of Array');
    }
    const length = total ?? list.reduce((sum, it) => sum + it.length, 0);
    const made = new Buffer(length);
    let at = 0;
    for (const part of list) {
      if (at >= length) {
        break;
      }
      const room = Math.min(part.length, length - at);
      made.set(part.subarray(0, room), at);
      at += room;
    }
    return made;
  }

  static compare(one, two) {
    return compared(one, two);
  }

  toString(encoding, start, end) {
    const name = named(encoding);
    const bytes = this.subarray(start ?? 0, end ?? this.length);
    switch (name) {
      case "utf8":
      case "utf-8":
        return decoderFor("utf-8").decode(bytes);
      case "hex": {
        let text = "";
        for (const byte of bytes) {
          text += byte.toString(16).padStart(2, "0");
        }
        return text;
      }
      case "base64":
        return base64(bytes);
      case "base64url":
        return base64(bytes).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
      case "latin1":
      case "binary":
        return Array.from(bytes, (byte) => String.fromCharCode(byte)).join("");
      case "ascii":
        return Array.from(bytes, (byte) => String.fromCharCode(byte & 0x7f)).join("");
      default:
        return decoderFor("utf-16le").decode(bytes);
    }
  }

  toJSON() {
    return { type: "Buffer", data: Array.from(this) };
  }

  equals(other) {
    return compared(this, other) === 0;
  }

  compare(other, targetStart, targetEnd, sourceStart, sourceEnd) {
    return compared(
      this.subarray(sourceStart ?? 0, sourceEnd ?? this.length),
      other.subarray(targetStart ?? 0, targetEnd ?? other.length),
    );
  }

  copy(target, targetStart = 0, sourceStart = 0, sourceEnd = this.length) {
    const bytes = this.subarray(sourceStart, sourceEnd);
    const room = Math.min(bytes.length, target.length - targetStart);
    target.set(bytes.subarray(0, room), targetStart);
    return room;
  }

  write(text, offset, length, encoding) {
    // Node's four argument dance, where any of the middle two may be
    // the encoding instead.
    if (typeof offset === "string") {
      encoding = offset;
      offset = 0;
      length = this.length;
    } else if (typeof length === "string") {
      encoding = length;
      length = this.length - offset;
    }
    const bytes = fromString(text, encoding);
    const room = Math.min(bytes.length, length ?? this.length - (offset ?? 0));
    this.set(bytes.subarray(0, room), offset ?? 0);
    return room;
  }

  fill(value, start = 0, end = this.length, encoding) {
    if (typeof value === "string") {
      const bytes = fromString(value, encoding);
      if (bytes.length === 0) {
        return this;
      }
      for (let at = start; at < end; at += 1) {
        this[at] = bytes[(at - start) % bytes.length];
      }
      return this;
    }
    if (ArrayBuffer.isView(value)) {
      for (let at = start; at < end; at += 1) {
        this[at] = value[(at - start) % value.length];
      }
      return this;
    }
    return super.fill(Number(value) & 0xff, start, end);
  }

  slice(start, end) {
    return asBuffer(this.subarray(start, end));
  }

  indexOf(value, from = 0, encoding) {
    const needle =
      typeof value === "string"
        ? fromString(value, encoding)
        : typeof value === "number"
          ? Uint8Array.of(value & 0xff)
          : value;
    if (needle.length === 0) {
      return 0;
    }
    outer: for (let at = Math.max(0, from); at + needle.length <= this.length; at += 1) {
      for (let step = 0; step < needle.length; step += 1) {
        if (this[at + step] !== needle[step]) {
          continue outer;
        }
      }
      return at;
    }
    return -1;
  }

  includes(value, from, encoding) {
    return this.indexOf(value, from, encoding) !== -1;
  }

  swap16() {
    return swapped(this, 2);
  }

  swap32() {
    return swapped(this, 4);
  }

  swap64() {
    return swapped(this, 8);
  }

  readUInt8(offset = 0) {
    return this[offset];
  }

  readInt8(offset = 0) {
    return view(this).getInt8(offset);
  }

  readUInt16BE(offset = 0) {
    return view(this).getUint16(offset, false);
  }

  readUInt16LE(offset = 0) {
    return view(this).getUint16(offset, true);
  }

  readInt16BE(offset = 0) {
    return view(this).getInt16(offset, false);
  }

  readInt16LE(offset = 0) {
    return view(this).getInt16(offset, true);
  }

  readUInt32BE(offset = 0) {
    return view(this).getUint32(offset, false);
  }

  readUInt32LE(offset = 0) {
    return view(this).getUint32(offset, true);
  }

  readInt32BE(offset = 0) {
    return view(this).getInt32(offset, false);
  }

  readInt32LE(offset = 0) {
    return view(this).getInt32(offset, true);
  }

  readBigUInt64BE(offset = 0) {
    return view(this).getBigUint64(offset, false);
  }

  readBigUInt64LE(offset = 0) {
    return view(this).getBigUint64(offset, true);
  }

  readBigInt64BE(offset = 0) {
    return view(this).getBigInt64(offset, false);
  }

  readBigInt64LE(offset = 0) {
    return view(this).getBigInt64(offset, true);
  }

  readFloatBE(offset = 0) {
    return view(this).getFloat32(offset, false);
  }

  readFloatLE(offset = 0) {
    return view(this).getFloat32(offset, true);
  }

  readDoubleBE(offset = 0) {
    return view(this).getFloat64(offset, false);
  }

  readDoubleLE(offset = 0) {
    return view(this).getFloat64(offset, true);
  }

  writeUInt8(value, offset = 0) {
    this[offset] = value & 0xff;
    return offset + 1;
  }

  writeInt8(value, offset = 0) {
    view(this).setInt8(offset, value);
    return offset + 1;
  }

  writeUInt16BE(value, offset = 0) {
    view(this).setUint16(offset, value, false);
    return offset + 2;
  }

  writeUInt16LE(value, offset = 0) {
    view(this).setUint16(offset, value, true);
    return offset + 2;
  }

  writeInt16BE(value, offset = 0) {
    view(this).setInt16(offset, value, false);
    return offset + 2;
  }

  writeInt16LE(value, offset = 0) {
    view(this).setInt16(offset, value, true);
    return offset + 2;
  }

  writeUInt32BE(value, offset = 0) {
    view(this).setUint32(offset, value, false);
    return offset + 4;
  }

  writeUInt32LE(value, offset = 0) {
    view(this).setUint32(offset, value, true);
    return offset + 4;
  }

  writeInt32BE(value, offset = 0) {
    view(this).setInt32(offset, value, false);
    return offset + 4;
  }

  writeInt32LE(value, offset = 0) {
    view(this).setInt32(offset, value, true);
    return offset + 4;
  }

  writeBigUInt64BE(value, offset = 0) {
    view(this).setBigUint64(offset, BigInt(value), false);
    return offset + 8;
  }

  writeBigUInt64LE(value, offset = 0) {
    view(this).setBigUint64(offset, BigInt(value), true);
    return offset + 8;
  }

  writeBigInt64BE(value, offset = 0) {
    view(this).setBigInt64(offset, BigInt(value), false);
    return offset + 8;
  }

  writeBigInt64LE(value, offset = 0) {
    view(this).setBigInt64(offset, BigInt(value), true);
    return offset + 8;
  }

  writeFloatBE(value, offset = 0) {
    view(this).setFloat32(offset, value, false);
    return offset + 4;
  }

  writeFloatLE(value, offset = 0) {
    view(this).setFloat32(offset, value, true);
    return offset + 4;
  }

  writeDoubleBE(value, offset = 0) {
    view(this).setFloat64(offset, value, false);
    return offset + 8;
  }

  writeDoubleLE(value, offset = 0) {
    view(this).setFloat64(offset, value, true);
    return offset + 8;
  }
}

// Node writes these two aliases on the prototype, and a package that
// reads a number without saying which order means big endian.
Buffer.prototype.readUIntBE = function readUIntBE(offset, length) {
  let value = 0;
  for (let step = 0; step < length; step += 1) {
    value = value * 256 + this[offset + step];
  }
  return value;
};
Buffer.prototype.readUIntLE = function readUIntLE(offset, length) {
  let value = 0;
  for (let step = length - 1; step >= 0; step -= 1) {
    value = value * 256 + this[offset + step];
  }
  return value;
};
Buffer.prototype.writeUIntBE = function writeUIntBE(value, offset, length) {
  for (let step = length - 1; step >= 0; step -= 1) {
    this[offset + step] = value & 0xff;
    value = Math.floor(value / 256);
  }
  return offset + length;
};
Buffer.prototype.writeUIntLE = function writeUIntLE(value, offset, length) {
  for (let step = 0; step < length; step += 1) {
    this[offset + step] = value & 0xff;
    value = Math.floor(value / 256);
  }
  return offset + length;
};

function view(bytes) {
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

function swapped(bytes, width) {
  if (bytes.length % width !== 0) {
    const wrong = new RangeError("Buffer size must be a multiple of " + width * 8 + "-bits");
    wrong.code = "ERR_INVALID_BUFFER_SIZE";
    throw wrong;
  }
  for (let at = 0; at < bytes.length; at += width) {
    for (let step = 0; step < width / 2; step += 1) {
      const one = bytes[at + step];
      bytes[at + step] = bytes[at + width - 1 - step];
      bytes[at + width - 1 - step] = one;
    }
  }
  return bytes;
}

/// The same bytes, seen as a Buffer, with no copy.
function asBuffer(bytes) {
  return new Buffer(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

function copied(bytes) {
  const made = new Buffer(bytes.length);
  made.set(bytes);
  return made;
}

function fromString(text, encoding) {
  const name = named(encoding);
  switch (name) {
    case "utf8":
    case "utf-8":
      return copied(encoder.encode(text));
    case "hex": {
      // Node stops at the first byte it cannot read rather than
      // throwing, and a package validating hex depends on the short
      // answer coming back.
      const clean = text.length % 2 === 0 ? text : text.slice(0, text.length - 1);
      const made = new Buffer(clean.length / 2);
      for (let at = 0; at < made.length; at += 1) {
        const byte = Number.parseInt(clean.slice(at * 2, at * 2 + 2), 16);
        if (Number.isNaN(byte)) {
          return asBuffer(made.subarray(0, at));
        }
        made[at] = byte;
      }
      return made;
    }
    case "base64":
    case "base64url": {
      const padded = text.replaceAll("-", "+").replaceAll("_", "/");
      const binary = atob(padded.padEnd(Math.ceil(padded.length / 4) * 4, "="));
      const made = new Buffer(binary.length);
      for (let at = 0; at < binary.length; at += 1) {
        made[at] = binary.charCodeAt(at);
      }
      return made;
    }
    case "latin1":
    case "binary":
    case "ascii": {
      const made = new Buffer(text.length);
      for (let at = 0; at < text.length; at += 1) {
        made[at] = text.charCodeAt(at) & 0xff;
      }
      return made;
    }
    default: {
      const made = new Buffer(text.length * 2);
      const into = view(made);
      for (let at = 0; at < text.length; at += 1) {
        into.setUint16(at * 2, text.charCodeAt(at), true);
      }
      return made;
    }
  }
}

function base64(bytes) {
  let binary = "";
  // A chunk at a time, because `String.fromCharCode(...bytes)` on a
  // megabyte of them is an argument list v8 will not take.
  for (let at = 0; at < bytes.length; at += 0x8000) {
    binary += String.fromCharCode.apply(null, bytes.subarray(at, at + 0x8000));
  }
  return btoa(binary);
}

function compared(one, two) {
  const length = Math.min(one.length, two.length);
  for (let at = 0; at < length; at += 1) {
    if (one[at] !== two[at]) {
      return one[at] < two[at] ? -1 : 1;
    }
  }
  return one.length === two.length ? 0 : one.length < two.length ? -1 : 1;
}

const constants = {
  MAX_LENGTH: 0x7fffffff,
  MAX_STRING_LENGTH: 0x1fffffe8,
};

const kMaxLength = constants.MAX_LENGTH;
const kStringMaxLength = constants.MAX_STRING_LENGTH;

/// Node kept this around from before `Buffer.from` existed and a few
/// packages still call it.
function SlowBuffer(size) {
  return Buffer.alloc(size);
}

function transcode() {
  throw new TypeError("node:buffer transcode is not implemented here");
}

function isUtf8(bytes) {
  try {
    decoderFor("utf-8").decode(bytes);
    return true;
  } catch {
    return false;
  }
}

function isAscii(bytes) {
  return Array.prototype.every.call(bytes, (byte) => byte < 0x80);
}

// Node's buffer module carries these four, and they are the global
// ones here rather than a second implementation of either. A local
// binding because an export has to name one.
const decode64 = globalThis.atob;
const encode64 = globalThis.btoa;
const blob = globalThis.Blob;
const file = globalThis.File;

export default {
  Buffer,
  SlowBuffer,
  constants,
  kMaxLength,
  kStringMaxLength,
  atob: decode64,
  btoa: encode64,
  Blob: blob,
  File: file,
  transcode,
  isUtf8,
  isAscii,
};
export {
  Buffer,
  SlowBuffer,
  constants,
  kMaxLength,
  kStringMaxLength,
  decode64 as atob,
  encode64 as btoa,
  blob as Blob,
  file as File,
  transcode,
  isUtf8,
  isAscii,
};
