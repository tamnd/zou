// node:events, which is the built in the rest of them are built on:
// a stream is an emitter, and so is half of what a package on the
// registry ships.
//
// The emitter is small on purpose. What is here is the whole of the
// listener API a package uses, the two error rules that are surprising
// if they are missing, and nothing about domains or async resources.

const kEvents = Symbol("events");

/// Node throws the argument back at the caller when it is not a
/// function, with this code on it, and libraries check the code.
function callable(listener) {
  if (typeof listener !== "function") {
    const wrong = new TypeError(
      `The "listener" argument must be of type function. Received ${typeof listener}`,
    );
    wrong.code = "ERR_INVALID_ARG_TYPE";
    throw wrong;
  }
  return listener;
}

class EventEmitter {
  constructor(options) {
    this[kEvents] = new Map();
    this._maxListeners = undefined;
    this[kCapture] = Boolean(options && options.captureRejections);
  }

  addListener(type, listener) {
    return added(this, type, listener, false);
  }

  on(type, listener) {
    return added(this, type, listener, false);
  }

  prependListener(type, listener) {
    return added(this, type, listener, true);
  }

  once(type, listener) {
    return added(this, type, wrapped(this, type, listener), false);
  }

  prependOnceListener(type, listener) {
    return added(this, type, wrapped(this, type, listener), true);
  }

  removeListener(type, listener) {
    callable(listener);
    const held = this[kEvents].get(type);
    if (held === undefined) {
      return this;
    }
    // Backwards, because node removes the most recently added match
    // and a listener added twice is two listeners.
    for (let index = held.length - 1; index >= 0; index -= 1) {
      if (held[index] === listener || held[index].listener === listener) {
        held.splice(index, 1);
        break;
      }
    }
    if (held.length === 0) {
      this[kEvents].delete(type);
    }
    return this;
  }

  off(type, listener) {
    return this.removeListener(type, listener);
  }

  removeAllListeners(type) {
    if (type === undefined) {
      this[kEvents].clear();
    } else {
      this[kEvents].delete(type);
    }
    return this;
  }

  emit(type, ...args) {
    const held = this[kEvents].get(type);
    // An `error` nobody is listening for is thrown rather than
    // swallowed, which is the rule a package relies on to make a
    // forgotten handler loud instead of silent.
    if (held === undefined || held.length === 0) {
      if (type === "error") {
        const why = args[0];
        throw why instanceof Error
          ? why
          : Object.assign(new Error(`Unhandled error. (${why})`), {
              code: "ERR_UNHANDLED_ERROR",
              context: why,
            });
      }
      return false;
    }
    // A copy, because a listener is allowed to add or remove listeners
    // while it is being called and node calls the set that existed
    // when the event was emitted.
    for (const listener of held.slice()) {
      const answer = listener.apply(this, args);
      if (this[kCapture] && answer !== null && typeof answer?.then === "function") {
        answer.then(undefined, (why) => this.emit("error", why));
      }
    }
    return true;
  }

  listeners(type) {
    return (this[kEvents].get(type) ?? []).map((it) => it.listener ?? it);
  }

  rawListeners(type) {
    return (this[kEvents].get(type) ?? []).slice();
  }

  listenerCount(type) {
    return (this[kEvents].get(type) ?? []).length;
  }

  eventNames() {
    return Array.from(this[kEvents].keys());
  }

  setMaxListeners(count) {
    this._maxListeners = count;
    return this;
  }

  getMaxListeners() {
    return this._maxListeners ?? EventEmitter.defaultMaxListeners;
  }
}

const kCapture = Symbol("captureRejections");

function added(emitter, type, listener, first) {
  callable(listener);
  // An emitter that was built without calling this constructor still
  // works, because a package subclassing one and forgetting `super()`
  // is a real thing and node survives it.
  if (emitter[kEvents] === undefined) {
    emitter[kEvents] = new Map();
  }
  emitter.emit?.("newListener", type, listener.listener ?? listener);
  const held = emitter[kEvents].get(type);
  if (held === undefined) {
    emitter[kEvents].set(type, [listener]);
  } else if (first) {
    held.unshift(listener);
  } else {
    held.push(listener);
  }
  return emitter;
}

/// A once listener is the listener with a wrapper around it that takes
/// it off first, and `listener` on the wrapper is how `removeListener`
/// and `listeners()` still see the function the caller passed.
function wrapped(emitter, type, listener) {
  callable(listener);
  const once = function (...args) {
    emitter.removeListener(type, once);
    return listener.apply(emitter, args);
  };
  once.listener = listener;
  return once;
}

EventEmitter.defaultMaxListeners = 10;
EventEmitter.captureRejectionSymbol = kCapture;
EventEmitter.errorMonitor = Symbol("events.errorMonitor");

/// `events.once(emitter, name)`, which is how a caller waits for one
/// event with a promise. An `error` while waiting rejects it, which is
/// the whole reason this is not three lines at the call site.
EventEmitter.once = function once(emitter, name, options = {}) {
  return new Promise((resolve, reject) => {
    const settle = (...args) => {
      emitter.removeListener?.("error", failed);
      resolve(args);
    };
    const failed = (why) => {
      emitter.removeListener?.(name, settle);
      reject(why);
    };
    if (typeof emitter.once === "function") {
      emitter.once(name, settle);
      if (name !== "error") {
        emitter.once("error", failed);
      }
    } else {
      emitter.addEventListener(name, (event) => resolve([event]), { once: true });
    }
    const signal = options.signal;
    if (signal) {
      signal.addEventListener("abort", () => reject(signal.reason), { once: true });
    }
  });
};

/// `events.on(emitter, name)`, an async iterator over every event of
/// one name. Unbounded, the way node's is: a consumer that stops
/// consuming is a queue that grows.
EventEmitter.on = function on(emitter, name) {
  const waiting = [];
  const queued = [];
  let stopped = null;
  emitter.on(name, (...args) => {
    const next = waiting.shift();
    if (next) {
      next.resolve({ value: args, done: false });
    } else {
      queued.push(args);
    }
  });
  emitter.on("error", (why) => {
    stopped = why;
    for (const next of waiting.splice(0)) {
      next.reject(why);
    }
  });
  return {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      if (queued.length > 0) {
        return Promise.resolve({ value: queued.shift(), done: false });
      }
      if (stopped !== null) {
        return Promise.reject(stopped);
      }
      return new Promise((resolve, reject) => waiting.push({ resolve, reject }));
    },
    return() {
      return Promise.resolve({ value: undefined, done: true });
    },
  };
};

EventEmitter.listenerCount = function listenerCount(emitter, type) {
  return emitter.listenerCount(type);
};

EventEmitter.setMaxListeners = function setMaxListeners() {};

// Node's own shape, where the module is the class and the class is on
// the module under two names, because all three are written in the
// wild and a package that picked one should not care which.
EventEmitter.EventEmitter = EventEmitter;
EventEmitter.default = EventEmitter;

export default EventEmitter;
export {
  EventEmitter,
  EventEmitter as EventEmitterAsyncResource,
  kCapture as captureRejectionSymbol,
};
export const once = EventEmitter.once;
export const on = EventEmitter.on;
export const listenerCount = EventEmitter.listenerCount;
export const setMaxListeners = EventEmitter.setMaxListeners;
export const defaultMaxListeners = EventEmitter.defaultMaxListeners;
export const errorMonitor = EventEmitter.errorMonitor;
