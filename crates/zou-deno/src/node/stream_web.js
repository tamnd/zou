// node:stream/web, which is node's name for the web streams. This
// runtime has them on the global, so the module is the global ones
// under the names node gives them and not a second implementation.

const ReadableStreamClass = globalThis.ReadableStream;
const WritableStreamClass = globalThis.WritableStream;
const TransformStreamClass = globalThis.TransformStream;

function absent(name) {
  return function () {
    throw new TypeError(`node:stream/web ${name} is not implemented here`);
  };
}

// The queueing strategies are two objects with a `size` on them, so
// they are here rather than missing: a package that hands one to a
// stream constructor is asking for a number back.
class CountQueuingStrategy {
  constructor(options) {
    this.highWaterMark = options?.highWaterMark ?? 1;
  }
  size() {
    return 1;
  }
}

class ByteLengthQueuingStrategy {
  constructor(options) {
    this.highWaterMark = options?.highWaterMark ?? 1;
  }
  size(chunk) {
    return chunk?.byteLength ?? 0;
  }
}

const ByteStream = absent("ReadableStreamBYOBReader");

export {
  ReadableStreamClass as ReadableStream,
  WritableStreamClass as WritableStream,
  TransformStreamClass as TransformStream,
  CountQueuingStrategy,
  ByteLengthQueuingStrategy,
  ByteStream as ReadableStreamBYOBReader,
};

export default {
  ReadableStream: ReadableStreamClass,
  WritableStream: WritableStreamClass,
  TransformStream: TransformStreamClass,
  CountQueuingStrategy,
  ByteLengthQueuingStrategy,
  ReadableStreamBYOBReader: ByteStream,
};
