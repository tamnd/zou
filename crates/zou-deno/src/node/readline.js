// node:readline, which is a stream cut into lines.
//
// The terminal half of node's module is not here and cannot be: a
// function has no tty, so there is no cursor to move, no history to
// walk and no completer to call. What is left is the half a package
// running without a terminal uses, which is the interesting one: an
// interface over an input, a `line` event for each line in it, an
// async iterator over the same lines, and a `question` that is
// answered by the next line rather than by somebody typing.
//
// The input may be a node readable, a web readable stream or anything
// else that iterates, because the three of them all turn up: a package
// written for node hands the first, a package written for this
// runtime hands the second, and a test hands an array.
//
// A line ends at a newline, and a carriage return in front of one is
// part of the ending rather than part of the line, which is what node
// does with a file written on the other kind of machine. The last line
// of an input that does not end in a newline is still a line.

import { EventEmitter } from "node:events";

/// A queue with a waiter, which is what sits between the loop reading
/// the input and whoever is reading the lines: neither of them can
/// wait on the other directly, because a line may arrive before it is
/// asked for and may be asked for before it arrives.
class Lines {
  #held = [];
  #waiting = [];
  #ended = false;

  push(line) {
    const waiter = this.#waiting.shift();
    if (waiter !== undefined) {
      waiter({ value: line, done: false });
      return;
    }
    this.#held.push(line);
  }

  end() {
    this.#ended = true;
    for (const waiter of this.#waiting.splice(0)) {
      waiter({ value: undefined, done: true });
    }
  }

  next() {
    if (this.#held.length > 0) {
      return Promise.resolve({ value: this.#held.shift(), done: false });
    }
    if (this.#ended) return Promise.resolve({ value: undefined, done: true });
    return new Promise((resolve) => this.#waiting.push(resolve));
  }
}

/// Bytes or text out of whatever was handed in, one piece at a time.
async function* pieces(input) {
  if (input === null || input === undefined) return;
  // A string iterates a character at a time, which is a line reader
  // taking the long way to the same answer.
  if (typeof input === "string") {
    yield input;
    return;
  }
  // A web readable stream, which is what `Response.body` is. Newer
  // ones iterate and older ones do not, so the reader is the way in.
  if (typeof input.getReader === "function") {
    const reader = input.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) return;
        yield value;
      }
    } finally {
      reader.releaseLock();
    }
    return;
  }
  if (typeof input[Symbol.asyncIterator] === "function" || typeof input[Symbol.iterator] === "function") {
    yield* input;
    return;
  }
  // A node readable old enough not to iterate, which is still an
  // emitter, so its events are the iteration.
  if (typeof input.on === "function") {
    const lines = new Lines();
    input.on("data", (chunk) => lines.push(chunk));
    input.on("end", () => lines.end());
    input.on("error", () => lines.end());
    for (;;) {
      const { value, done } = await lines.next();
      if (done) return;
      yield value;
    }
  }
  throw new TypeError("The \"input\" argument must be a readable stream");
}

const decoder = new TextDecoder();

function textOf(piece) {
  if (typeof piece === "string") return piece;
  if (ArrayBuffer.isView(piece) || piece instanceof ArrayBuffer) {
    return decoder.decode(piece, { stream: true });
  }
  return String(piece);
}

export class Interface extends EventEmitter {
  #lines = new Lines();
  #waiting = [];
  #paused = false;
  #closed = false;
  #held = [];

  constructor(input, output, completer, terminal) {
    super();
    const options = input !== null && typeof input === "object" && input.input !== undefined
      ? input
      : { input, output, completer, terminal };
    this.input = options.input;
    this.output = options.output ?? null;
    // No tty means no terminal, whatever the caller asked for, and
    // saying so is better than half a line editor.
    this.terminal = false;
    this.line = "";
    this.cursor = 0;
    this._prompt = options.prompt ?? "> ";
    this.#pump();
  }

  async #pump() {
    let rest = "";
    try {
      for await (const piece of pieces(this.input)) {
        rest += textOf(piece);
        let at = rest.indexOf("\n");
        while (at !== -1) {
          const line = rest.slice(0, at);
          rest = rest.slice(at + 1);
          this.#line(line.endsWith("\r") ? line.slice(0, -1) : line);
          at = rest.indexOf("\n");
        }
      }
    } catch (why) {
      this.emit("error", why);
    }
    if (rest.length > 0) {
      this.#line(rest.endsWith("\r") ? rest.slice(0, -1) : rest);
    }
    this.close();
  }

  /// One line, to whoever is waiting for one: a `question` first,
  /// because a caller that asked for an answer is holding the line
  /// that arrives, and everybody else after it.
  #line(line) {
    const asked = this.#waiting.shift();
    if (asked !== undefined) {
      asked(line);
      return;
    }
    // A paused interface still has its input read, because the input
    // may be a socket that will not wait. What is held back is the
    // event, which is what a caller pauses to stop.
    if (this.#paused) {
      this.#held.push(line);
      return;
    }
    this.#lines.push(line);
    this.emit("line", line);
  }

  question(query, options, back) {
    if (typeof options === "function") {
      back = options;
      options = {};
    }
    this.write(query);
    const answer = new Promise((resolve) => this.#waiting.push(resolve));
    if (typeof back === "function") {
      answer.then(back);
      return undefined;
    }
    // The callback module returns nothing from `question`, and the
    // promises one returns the promise. A caller of the first that
    // forgot the callback gets the second's answer rather than a
    // sentence about arity.
    return answer;
  }

  prompt() {
    this.write(this._prompt);
  }

  setPrompt(prompt) {
    this._prompt = String(prompt);
  }

  getPrompt() {
    return this._prompt;
  }

  write(text) {
    if (this.output !== null && this.output !== undefined && text !== undefined) {
      if (typeof this.output.write === "function") this.output.write(String(text));
    }
  }

  pause() {
    this.#paused = true;
    this.emit("pause");
    return this;
  }

  resume() {
    this.#paused = false;
    for (const line of this.#held.splice(0)) {
      this.#lines.push(line);
      this.emit("line", line);
    }
    this.emit("resume");
    return this;
  }

  close() {
    if (this.#closed) return;
    this.#closed = true;
    for (const asked of this.#waiting.splice(0)) asked(undefined);
    this.#lines.end();
    this.emit("close");
  }

  get closed() {
    return this.#closed;
  }

  [Symbol.asyncIterator]() {
    return {
      next: () => this.#lines.next(),
      return: () => {
        this.close();
        return Promise.resolve({ value: undefined, done: true });
      },
      [Symbol.asyncIterator]() {
        return this;
      },
    };
  }
}

export function createInterface(input, output, completer, terminal) {
  return new Interface(input, output, completer, terminal);
}

/// The cursor calls, which have nothing to move. Node answers with
/// whether it wrote anything, and it did not, so this is `false` and
/// the callback still runs, which is the contract a caller waits on.
function nowhere(back) {
  if (typeof back === "function") queueMicrotask(back);
  return false;
}

export function clearLine(stream, direction, back) {
  return nowhere(back);
}
export function clearScreenDown(stream, back) {
  return nowhere(back);
}
export function cursorTo(stream, x, y, back) {
  return nowhere(typeof y === "function" ? y : back);
}
export function moveCursor(stream, dx, dy, back) {
  return nowhere(back);
}
export function emitKeypressEvents() {}

export const promises = {
  Interface,
  createInterface,
};

export default {
  Interface,
  createInterface,
  clearLine,
  clearScreenDown,
  cursorTo,
  moveCursor,
  emitKeypressEvents,
  promises,
};
