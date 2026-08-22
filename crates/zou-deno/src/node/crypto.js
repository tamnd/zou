// node:crypto, the part of it a package on the registry uses: random
// bytes, a hash, an hmac, and web crypto under its node name.
//
// A hash here is synchronous, which `crypto.subtle.digest` is not, so
// it goes through the same op the subtle one does rather than through
// a promise a caller cannot wait for. The bytes are held until
// `digest()` is called, which is not how a streaming hash is meant to
// work and is exactly what node's own object looks like from outside.
//
// What is missing is missing by name: no ciphers, no key derivation,
// no certificates, and md5 refused rather than quietly turned into
// something else.

import { Buffer } from "node:buffer";

const ops = Deno.core.ops;
const webcrypto = globalThis.crypto;

/// The names node takes for a hash, and what this runtime calls them.
const HASHES = new Map([
  ["sha1", "SHA-1"],
  ["sha-1", "SHA-1"],
  ["sha256", "SHA-256"],
  ["sha-256", "SHA-256"],
  ["sha384", "SHA-384"],
  ["sha-384", "SHA-384"],
  ["sha512", "SHA-512"],
  ["sha-512", "SHA-512"],
]);

function hashNamed(algorithm) {
  const found = HASHES.get(String(algorithm).toLowerCase());
  if (found === undefined) {
    const wrong = new Error(`Digest method not supported: ${algorithm}`);
    wrong.code = "ERR_CRYPTO_INVALID_DIGEST";
    throw wrong;
  }
  return found;
}

function bytesOf(data, encoding) {
  if (typeof data === "string") {
    return Buffer.from(data, encoding ?? "utf8");
  }
  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  }
  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data);
  }
  throw new TypeError("data must be a string, a Buffer or an ArrayBuffer");
}

function encoded(bytes, encoding) {
  const buffer = Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return encoding === undefined || encoding === null || encoding === "buffer"
    ? buffer
    : buffer.toString(encoding);
}

class Hash {
  #algorithm;
  #parts = [];

  constructor(algorithm) {
    this.#algorithm = hashNamed(algorithm);
  }

  update(data, encoding) {
    this.#parts.push(bytesOf(data, encoding));
    return this;
  }

  digest(encoding) {
    return encoded(ops.op_zou_digest(this.#algorithm, joined(this.#parts)), encoding);
  }

  copy() {
    // Same algorithm and the same bytes so far, and from here the two
    // are independent, which is the whole of what copying one means.
    const made = new Hash(this.#algorithm);
    made.#parts = this.#parts.slice();
    return made;
  }
}

class Hmac {
  #algorithm;
  #key;
  #parts = [];

  constructor(algorithm, key, options) {
    this.#algorithm = hashNamed(algorithm);
    this.#key = bytesOf(key, options?.encoding);
  }

  update(data, encoding) {
    this.#parts.push(bytesOf(data, encoding));
    return this;
  }

  digest(encoding) {
    return encoded(ops.op_zou_sign(this.#algorithm, this.#key, joined(this.#parts)), encoding);
  }
}

function joined(parts) {
  if (parts.length === 1) {
    return parts[0];
  }
  const total = parts.reduce((sum, it) => sum + it.length, 0);
  const all = new Uint8Array(total);
  let at = 0;
  for (const part of parts) {
    all.set(part, at);
    at += part.length;
  }
  return all;
}

export function createHash(algorithm) {
  return new Hash(algorithm);
}

export function createHmac(algorithm, key, options) {
  return new Hmac(algorithm, key, options);
}

export function randomBytes(size, back) {
  const made = Buffer.allocUnsafe(size);
  // getRandomValues has a quota per call, and node has none, so this
  // fills in blocks the standard allows.
  for (let at = 0; at < size; at += 65536) {
    webcrypto.getRandomValues(made.subarray(at, Math.min(at + 65536, size)));
  }
  if (typeof back === "function") {
    queueMicrotask(() => back(null, made));
    return undefined;
  }
  return made;
}

export function randomFillSync(bytes, offset = 0, size = bytes.length - offset) {
  const into = new Uint8Array(bytes.buffer, bytes.byteOffset + offset, size);
  webcrypto.getRandomValues(into);
  return bytes;
}

export function randomFill(bytes, offset, size, back) {
  if (typeof offset === "function") {
    back = offset;
    offset = 0;
    size = bytes.length;
  } else if (typeof size === "function") {
    back = size;
    size = bytes.length - offset;
  }
  randomFillSync(bytes, offset, size);
  queueMicrotask(() => back(null, bytes));
}

export function randomUUID() {
  return webcrypto.randomUUID();
}

export function randomInt(min, max, back) {
  if (typeof max === "function") {
    back = max;
    max = min;
    min = 0;
  }
  if (max === undefined) {
    max = min;
    min = 0;
  }
  const range = max - min;
  if (range <= 0) {
    throw new RangeError("The value of max must be greater than the value of min");
  }
  // Rejection sampling, so every value in the range is as likely as
  // every other one. A modulo would make the low ones commoner.
  const bits = Math.ceil(Math.log2(range));
  const bytes = Math.ceil(bits / 8);
  const limit = 2 ** (bytes * 8);
  const drop = limit - (limit % range);
  let value;
  do {
    const made = randomBytes(bytes);
    value = 0;
    for (const byte of made) {
      value = value * 256 + byte;
    }
  } while (value >= drop);
  const answer = min + (value % range);
  if (typeof back === "function") {
    queueMicrotask(() => back(null, answer));
    return undefined;
  }
  return answer;
}

/// Whether two buffers are the same, in a time that does not say where
/// the first difference is. Javascript cannot promise that the way the
/// host can, so this is the accumulate trick: every byte is read
/// whatever the earlier ones said.
export function timingSafeEqual(one, two) {
  if (one.length !== two.length) {
    throw new RangeError("Input buffers must have the same byte length");
  }
  let same = 0;
  for (let at = 0; at < one.length; at += 1) {
    same |= one[at] ^ two[at];
  }
  return same === 0;
}

export function getHashes() {
  return ["sha1", "sha256", "sha384", "sha512"];
}

export function getRandomValues(into) {
  return webcrypto.getRandomValues(into);
}

function absent(name) {
  return function () {
    throw new TypeError(`node:crypto ${name} is not implemented here`);
  };
}

export const createCipheriv = absent("createCipheriv");
export const createDecipheriv = absent("createDecipheriv");
export const createSign = absent("createSign");
export const createVerify = absent("createVerify");
export const generateKeyPair = absent("generateKeyPair");
export const generateKeyPairSync = absent("generateKeyPairSync");
export const createPrivateKey = absent("createPrivateKey");
export const createPublicKey = absent("createPublicKey");
export const createSecretKey = absent("createSecretKey");
export const pbkdf2 = absent("pbkdf2");
export const pbkdf2Sync = absent("pbkdf2Sync");
export const scrypt = absent("scrypt");
export const scryptSync = absent("scryptSync");

export const subtle = webcrypto.subtle;
export const constants = {};
export { webcrypto, Hash, Hmac };

export default {
  createHash,
  createHmac,
  randomBytes,
  randomFill,
  randomFillSync,
  randomUUID,
  randomInt,
  timingSafeEqual,
  getHashes,
  getRandomValues,
  createCipheriv,
  createDecipheriv,
  createSign,
  createVerify,
  generateKeyPair,
  generateKeyPairSync,
  createPrivateKey,
  createPublicKey,
  createSecretKey,
  pbkdf2,
  pbkdf2Sync,
  scrypt,
  scryptSync,
  subtle,
  constants,
  webcrypto,
  Hash,
  Hmac,
};
