// node:timers, which is the global timers under their module names.
//
// Node hands back a Timeout object rather than a number, and the thing
// packages do with it is `unref()` and `clearTimeout`. The number this
// runtime has works for the second, so it is wrapped in an object that
// answers the first and turns back into the number when a caller does
// arithmetic on it.

const setTimeoutGlobal = globalThis.setTimeout;
const setIntervalGlobal = globalThis.setInterval;
const clearTimeoutGlobal = globalThis.clearTimeout;
const clearIntervalGlobal = globalThis.clearInterval;

class Timeout {
  constructor(id) {
    this.id = id;
  }
  /// A timer here only runs while there is a call for it to run in, so
  /// there is nothing for either of these to hold open or let go of.
  unref() {
    return this;
  }
  ref() {
    return this;
  }
  hasRef() {
    return true;
  }
  refresh() {
    return this;
  }
  close() {
    clearTimeoutGlobal(this.id);
  }
  valueOf() {
    return this.id;
  }
  [Symbol.toPrimitive]() {
    return this.id;
  }
}

function idOf(timer) {
  return timer instanceof Timeout ? timer.id : timer;
}

export function setTimeout(work, delay, ...args) {
  return new Timeout(setTimeoutGlobal(() => work(...args), delay));
}

export function setInterval(work, delay, ...args) {
  return new Timeout(setIntervalGlobal(() => work(...args), delay));
}

export function setImmediate(work, ...args) {
  return new Timeout(setTimeoutGlobal(() => work(...args), 0));
}

export function clearTimeout(timer) {
  clearTimeoutGlobal(idOf(timer));
}

export function clearInterval(timer) {
  clearIntervalGlobal(idOf(timer));
}

export function clearImmediate(timer) {
  clearTimeoutGlobal(idOf(timer));
}

export { Timeout };

export default {
  setTimeout,
  setInterval,
  setImmediate,
  clearTimeout,
  clearInterval,
  clearImmediate,
  Timeout,
};
