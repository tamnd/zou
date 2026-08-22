// node:stream, in the shape a package uses one: something to push
// bytes into, something to read them out of, and a transform in the
// middle.
//
// This is not node's implementation. There is no high water mark that
// means anything, no corking, no `_readableState` for a package to
// reach into, and backpressure is the promise `write` gives back
// rather than a queue with a limit on it. What is here is the API
// surface a library touches: `push`, `read`, `pipe`, `write`, `end`,
// the events those emit, the async iterator, and the two bridges to
// the web streams this runtime actually has.
//
// A package that reaches into node's internals will notice. That is
// the trade, and the alternative is several thousand lines that would
// still not be node's.

import EventEmitter from "node:events";
import { Buffer } from "node:buffer";

const kState = Symbol("state");

function stateOf(stream) {
  if (stream[kState] === undefined) {
    stream[kState] = {
      queue: [],
      flowing: false,
      ended: false,
      endEmitted: false,
      destroyed: false,
      reading: false,
      encoding: null,
      objectMode: false,
      waiting: [],
      finished: false,
      writes: [],
    };
  }
  return stream[kState];
}

class Stream extends EventEmitter {}

class Readable extends Stream {
  constructor(options = {}) {
    super();
    const state = stateOf(this);
    state.objectMode = Boolean(options.objectMode);
    state.encoding = options.encoding ?? null;
    if (typeof options.read === "function") {
      this._read = options.read;
    }
    if (typeof options.destroy === "function") {
      this._destroy = options.destroy;
    }
    this.readable = true;
  }

  _read() {}

  _destroy(why, back) {
    back(why);
  }

  push(chunk) {
    const state = stateOf(this);
    if (chunk === null) {
      state.ended = true;
      wake(state);
      // The end is emitted once everything pushed before it has been
      // taken, which is what makes `data` then `end` the right order.
      if (state.queue.length === 0) {
        finish(this);
      }
      return false;
    }
    const value = state.objectMode ? chunk : bytes(chunk, state.encoding);
    if (state.flowing) {
      this.emit("data", value);
    } else {
      state.queue.push(value);
      this.emit("readable");
    }
    wake(state);
    return true;
  }

  unshift(chunk) {
    const state = stateOf(this);
    state.queue.unshift(state.objectMode ? chunk : bytes(chunk, state.encoding));
    wake(state);
    return true;
  }

  read() {
    const state = stateOf(this);
    if (state.queue.length === 0) {
      if (!state.reading) {
        state.reading = true;
        this._read();
        state.reading = false;
      }
      if (state.queue.length === 0) {
        if (state.ended) {
          finish(this);
        }
        return null;
      }
    }
    const value = state.queue.shift();
    if (state.queue.length === 0 && state.ended) {
      finish(this);
    }
    return value;
  }

  setEncoding(encoding) {
    stateOf(this).encoding = encoding;
    return this;
  }

  pause() {
    stateOf(this).flowing = false;
    this.emit("pause");
    return this;
  }

  resume() {
    const state = stateOf(this);
    if (state.flowing) {
      return this;
    }
    state.flowing = true;
    this.emit("resume");
    // Whatever was pushed before anybody was listening goes out now,
    // in order, and then the source is asked for more.
    queueMicrotask(() => {
      while (state.flowing && state.queue.length > 0) {
        this.emit("data", state.queue.shift());
      }
      if (state.ended) {
        finish(this);
      } else if (state.flowing) {
        this._read();
      }
    });
    return this;
  }

  isPaused() {
    return !stateOf(this).flowing;
  }

  on(type, listener) {
    const answer = super.on(type, listener);
    // A `data` listener is what puts a stream into flowing mode, which
    // is the one piece of node's stream behaviour every package
    // depends on without saying so.
    if (type === "data") {
      this.resume();
    }
    return answer;
  }

  pipe(destination, options = {}) {
    this.on("data", (chunk) => {
      if (destination.write(chunk) === false) {
        this.pause();
        destination.once("drain", () => this.resume());
      }
    });
    this.on("end", () => {
      if (options.end !== false) {
        destination.end();
      }
    });
    this.on("error", (why) => destination.destroy?.(why));
    destination.emit("pipe", this);
    return destination;
  }

  unpipe() {
    return this;
  }

  destroy(why) {
    const state = stateOf(this);
    if (state.destroyed) {
      return this;
    }
    state.destroyed = true;
    this.readable = false;
    this._destroy(why ?? null, (thrown) => {
      if (thrown) {
        this.emit("error", thrown);
      }
      this.emit("close");
    });
    wake(state);
    return this;
  }

  get destroyed() {
    return stateOf(this).destroyed;
  }

  get readableEnded() {
    return stateOf(this).endEmitted;
  }

  get readableFlowing() {
    return stateOf(this).flowing;
  }

  [Symbol.asyncIterator]() {
    const state = stateOf(this);
    const stream = this;
    let failed = null;
    this.on("error", (why) => {
      failed = why;
      wake(state);
    });
    return {
      [Symbol.asyncIterator]() {
        return this;
      },
      async next() {
        for (;;) {
          if (failed !== null) {
            const why = failed;
            failed = null;
            throw why;
          }
          if (state.queue.length > 0) {
            return { value: state.queue.shift(), done: false };
          }
          if (state.ended || state.destroyed) {
            finish(stream);
            return { value: undefined, done: true };
          }
          stream._read();
          if (state.queue.length > 0 || state.ended) {
            continue;
          }
          await new Promise((resolve) => state.waiting.push(resolve));
        }
      },
      async return() {
        stream.destroy();
        return { value: undefined, done: true };
      },
    };
  }

  /// Whatever can be iterated, as a stream. An async iterator too,
  /// which is what makes `Readable.from(response.body)` work.
  static from(source) {
    const made = new Readable({ objectMode: true });
    (async () => {
      try {
        for await (const chunk of source) {
          made.push(chunk);
        }
        made.push(null);
      } catch (why) {
        made.destroy(why);
        made.emit("error", why);
      }
    })();
    return made;
  }

  /// The bridge into the streams this runtime has, which is where a
  /// body coming back from `fetch` meets a package written for node.
  static fromWeb(readable) {
    return Readable.from(iterate(readable));
  }

  static toWeb(readable) {
    return new ReadableStream({
      start(controller) {
        readable.on("data", (chunk) => controller.enqueue(chunk));
        readable.on("end", () => controller.close());
        readable.on("error", (why) => controller.error(why));
      },
      cancel() {
        readable.destroy();
      },
    });
  }
}

class Writable extends Stream {
  constructor(options = {}) {
    super();
    const state = stateOf(this);
    state.objectMode = Boolean(options.objectMode);
    if (typeof options.write === "function") {
      this._write = options.write;
    }
    if (typeof options.final === "function") {
      this._final = options.final;
    }
    if (typeof options.destroy === "function") {
      this._destroy = options.destroy;
    }
    this.writable = true;
  }

  _write(chunk, encoding, back) {
    back();
  }

  _final(back) {
    back();
  }

  _destroy(why, back) {
    back(why);
  }

  write(chunk, encoding, back) {
    if (typeof encoding === "function") {
      back = encoding;
      encoding = null;
    }
    const state = stateOf(this);
    if (state.finished) {
      const why = new Error("write after end");
      why.code = "ERR_STREAM_WRITE_AFTER_END";
      this.emit("error", why);
      return false;
    }
    const value = state.objectMode ? chunk : bytes(chunk, encoding);
    this._write(value, encoding ?? "buffer", (why) => {
      if (why) {
        this.emit("error", why);
      }
      back?.(why ?? null);
      // Nothing is queued here, so a writer is always ready for more
      // and `drain` is emitted for whoever paused on the last answer.
      this.emit("drain");
    });
    return true;
  }

  end(chunk, encoding, back) {
    if (typeof chunk === "function") {
      back = chunk;
      chunk = undefined;
    } else if (typeof encoding === "function") {
      back = encoding;
      encoding = null;
    }
    if (chunk !== undefined && chunk !== null) {
      this.write(chunk, encoding);
    }
    const state = stateOf(this);
    if (state.finished) {
      return this;
    }
    state.finished = true;
    this.writable = false;
    this._final((why) => {
      if (why) {
        this.emit("error", why);
        return;
      }
      this.emit("finish");
      this.emit("close");
      back?.();
    });
    return this;
  }

  destroy(why) {
    const state = stateOf(this);
    if (state.destroyed) {
      return this;
    }
    state.destroyed = true;
    this.writable = false;
    this._destroy(why ?? null, (thrown) => {
      if (thrown) {
        this.emit("error", thrown);
      }
      this.emit("close");
    });
    return this;
  }

  get destroyed() {
    return stateOf(this).destroyed;
  }

  get writableEnded() {
    return stateOf(this).finished;
  }

  static toWeb(writable) {
    return new WritableStream({
      write(chunk) {
        writable.write(chunk);
      },
      close() {
        writable.end();
      },
      abort(why) {
        writable.destroy(why);
      },
    });
  }

  static fromWeb(writable) {
    const writer = writable.getWriter();
    return new Writable({
      write(chunk, encoding, back) {
        writer.write(chunk).then(() => back(), back);
      },
      final(back) {
        writer.close().then(() => back(), back);
      },
    });
  }
}

/// Both halves on one object. The readable half is inherited and the
/// writable half is copied onto the prototype, which is how node builds
/// this one too.
class Duplex extends Readable {
  constructor(options = {}) {
    super(options);
    if (typeof options.write === "function") {
      this._write = options.write;
    }
    if (typeof options.final === "function") {
      this._final = options.final;
    }
    this.writable = true;
  }
}

for (const name of ["_write", "_final", "write", "end", "writableEnded"]) {
  const held = Object.getOwnPropertyDescriptor(Writable.prototype, name);
  if (held !== undefined) {
    Object.defineProperty(Duplex.prototype, name, held);
  }
}

class Transform extends Duplex {
  constructor(options = {}) {
    super(options);
    if (typeof options.transform === "function") {
      this._transform = options.transform;
    }
    if (typeof options.flush === "function") {
      this._flush = options.flush;
    }
  }

  _transform(chunk, encoding, back) {
    back(null, chunk);
  }

  _flush(back) {
    back();
  }

  _write(chunk, encoding, back) {
    this._transform(chunk, encoding, (why, made) => {
      if (made !== undefined && made !== null) {
        this.push(made);
      }
      back(why);
    });
  }

  _final(back) {
    this._flush((why, made) => {
      if (made !== undefined && made !== null) {
        this.push(made);
      }
      this.push(null);
      back(why);
    });
  }
}

class PassThrough extends Transform {}

function finish(stream) {
  const state = stateOf(stream);
  if (state.endEmitted) {
    return;
  }
  state.endEmitted = true;
  stream.readable = false;
  queueMicrotask(() => stream.emit("end"));
}

/// Everybody waiting on the async iterator, told there is something to
/// look at. Which there may not be, and the loop there checks.
function wake(state) {
  for (const resolve of state.waiting.splice(0)) {
    resolve();
  }
}

function bytes(chunk, encoding) {
  if (typeof chunk === "string") {
    return Buffer.from(chunk, encoding && encoding !== "buffer" ? encoding : "utf8");
  }
  return chunk;
}

async function* iterate(readable) {
  const reader = readable.getReader();
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) {
        return;
      }
      yield value;
    }
  } finally {
    reader.releaseLock?.();
  }
}

/// One callback when a stream is done, whichever way it was done.
export function finished(stream, options, back) {
  if (typeof options === "function") {
    back = options;
  }
  let settled = false;
  const settle = (why) => {
    if (!settled) {
      settled = true;
      back(why ?? null);
    }
  };
  stream.on("end", () => settle(null));
  stream.on("finish", () => settle(null));
  stream.on("close", () => settle(null));
  stream.on("error", (why) => settle(why));
  return () => {
    settled = true;
  };
}

/// A chain of pipes with one place to hear that any of them failed,
/// which is the whole reason this exists rather than three `.pipe`
/// calls.
export function pipeline(...args) {
  const back = typeof args[args.length - 1] === "function" ? args.pop() : null;
  const streams = args.flat();
  const last = streams[streams.length - 1];
  let failed = null;
  const fail = (why) => {
    if (failed === null) {
      failed = why;
      back?.(why);
    }
  };
  for (let at = 0; at < streams.length - 1; at += 1) {
    streams[at].on("error", fail);
    streams[at].pipe(streams[at + 1]);
  }
  last.on("error", fail);
  finished(last, (why) => {
    if (why) {
      fail(why);
    } else if (failed === null) {
      back?.(null);
    }
  });
  return last;
}

const stream = {
  Stream,
  Readable,
  Writable,
  Duplex,
  Transform,
  PassThrough,
  pipeline,
  finished,
};
// Node's module is the Stream class with everything hung off it, and a
// package that did `require('stream').Readable` after a bundler turned
// it into a default import needs both spellings to be the same thing.
Object.assign(Stream, stream);
Stream.default = Stream;

export default Stream;
export { Stream, Readable, Writable, Duplex, Transform, PassThrough };
