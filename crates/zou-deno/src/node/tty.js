// node:tty, which is the answer a function gives a package that asks
// whether anybody is watching it.
//
// Nothing here is a terminal. A function is called over a socket and
// what it prints is a log line somebody reads later, so `isatty` is
// false for every descriptor and the two streams say the same thing
// about themselves. That is what packages want out of this module:
// `supports-color` asks `isatty(1)` and then asks the stream how many
// colours it has, and a logger that colours its output when a person
// is watching and leaves it plain when it is being piped somewhere is
// the whole of the difference.
//
// The shape is the work rather than the behaviour. `columns` and
// `rows` are here because a package that decided not to colour its
// output still lays it out, and a cursor move on a stream that is not
// a terminal writes nothing rather than writing an escape into a log.

import { Readable, Writable } from "node:stream";

/// Where a descriptor's bytes go. Node hands out a stream per fd and a
/// package that built its own writes to 1 or 2 meaning the same two
/// places the console goes.
function outputFor(fd) {
  return fd === 2 ? globalThis.process.stderr : globalThis.process.stdout;
}

export function isatty() {
  return false;
}

export class WriteStream extends Writable {
  constructor(fd = 1) {
    super();
    this.fd = fd;
    // Not a terminal, and every question below is that answer again in
    // the shape whoever asked it wanted.
    this.isTTY = false;
    this.columns = 80;
    this.rows = 24;
  }

  _write(chunk, encoding, back) {
    outputFor(this.fd).write(chunk);
    back();
  }

  getColorDepth() {
    // Two colours is node's answer for a stream with no colour in it,
    // and it is the one `supports-color` reads as none.
    return 1;
  }

  hasColors(count) {
    return count === undefined ? false : Number(count) <= 2;
  }

  getWindowSize() {
    return [this.columns, this.rows];
  }

  /// The four cursor moves, which write nothing. An escape sequence in
  /// a log file is noise in a place nobody can act on it.
  clearLine(direction, back) {
    return done(back);
  }

  clearScreenDown(back) {
    return done(back);
  }

  cursorTo(x, y, back) {
    return done(typeof y === "function" ? y : back);
  }

  moveCursor(dx, dy, back) {
    return done(back);
  }
}

export class ReadStream extends Readable {
  constructor(fd = 0) {
    super();
    this.fd = fd;
    this.isTTY = false;
    this.isRaw = false;
  }

  _read() {
    // Nothing is ever typed at a function.
    this.push(null);
  }

  setRawMode(raw) {
    this.isRaw = Boolean(raw);
    return this;
  }
}

function done(back) {
  if (typeof back === "function") queueMicrotask(back);
  return true;
}

export default { ReadStream, WriteStream, isatty };
