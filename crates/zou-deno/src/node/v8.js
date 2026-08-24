// node:v8, which for a package is two functions: a value written out
// as bytes and read back from them.
//
// It is the same serializer `structuredClone` goes through, because
// there is only one and it belongs to the engine. So what survives a
// trip through here is what survives a clone: a cycle stays a cycle, a
// value that appears twice arrives as one object twice, and a Map, a
// Set, a Date, a RegExp, a buffer and a BigInt arrive as themselves. A
// function or a symbol is refused, in v8's words rather than this
// file's.
//
// The bytes are v8's own format and carry its version in front of
// them, which is worth knowing before writing them anywhere they
// outlive the process that wrote them. Node says the same thing about
// its own.
//
// The rest of node's module is about the engine a program is running
// inside: heap statistics, flags, snapshots, coverage. A function does
// not own the isolate it is in, and each of those says so by name.

import { Buffer } from "node:buffer";

const core = Deno.core;

export function serialize(value) {
  const bytes = core.serialize(value, undefined, (message) => {
    throw new Error(message);
  });
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

export function deserialize(bytes) {
  const held = ArrayBuffer.isView(bytes)
    ? new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    : new Uint8Array(bytes);
  return core.deserialize(held, undefined);
}

/// Node's two classes, which are the two functions with a place to
/// hang a subclass's hooks off. The hooks a subclass overrides are
/// about host objects and shared array buffers, neither of which
/// exists here, so what is left is write and read.
export class Serializer {
  #value;
  #wrote = null;

  writeValue(value) {
    this.#value = value;
    this.#wrote = null;
    return true;
  }

  releaseBuffer() {
    if (this.#wrote === null) this.#wrote = serialize(this.#value);
    return this.#wrote;
  }

  writeHeader() {}
  writeUint32() {}
  writeUint64() {}
  writeDouble() {}
  writeRawBytes() {}
  transferArrayBuffer() {}
  setTreatArrayBufferViewsAsHostObjects() {}
}

export class Deserializer {
  #bytes;

  constructor(bytes) {
    this.#bytes = bytes;
  }

  readHeader() {
    return true;
  }

  readValue() {
    return deserialize(this.#bytes);
  }

  readUint32() {
    return 0;
  }
  readUint64() {
    return [0, 0];
  }
  readDouble() {
    return 0;
  }
  readRawBytes() {
    return Buffer.alloc(0);
  }
  transferArrayBuffer() {}
}

export class DefaultSerializer extends Serializer {}
export class DefaultDeserializer extends Deserializer {}

/// The isolate this function is running in belongs to the server and
/// not to the function, so everything about it refuses by name rather
/// than answering with a number nobody should act on.
function notYours(name) {
  return function () {
    throw new Error(`node:v8 ${name} is about the isolate, which a function does not own`);
  };
}

export const getHeapStatistics = notYours("getHeapStatistics");
export const getHeapSpaceStatistics = notYours("getHeapSpaceStatistics");
export const getHeapCodeStatistics = notYours("getHeapCodeStatistics");
export const getHeapSnapshot = notYours("getHeapSnapshot");
export const writeHeapSnapshot = notYours("writeHeapSnapshot");
export const setFlagsFromString = notYours("setFlagsFromString");
export const takeCoverage = notYours("takeCoverage");
export const stopCoverage = notYours("stopCoverage");
export const setHeapSnapshotNearHeapLimit = notYours("setHeapSnapshotNearHeapLimit");

/// The one thing node hangs here that is neither: a deep copy, which
/// is the platform's own and is the same serializer underneath.
export const promiseHooks = {
  onInit: () => () => {},
  onSettled: () => () => {},
  onBefore: () => () => {},
  onAfter: () => () => {},
  createHook: () => () => {},
};

export default {
  serialize,
  deserialize,
  Serializer,
  Deserializer,
  DefaultSerializer,
  DefaultDeserializer,
  getHeapStatistics,
  getHeapSpaceStatistics,
  getHeapCodeStatistics,
  getHeapSnapshot,
  writeHeapSnapshot,
  setFlagsFromString,
  takeCoverage,
  stopCoverage,
  setHeapSnapshotNearHeapLimit,
  promiseHooks,
};
