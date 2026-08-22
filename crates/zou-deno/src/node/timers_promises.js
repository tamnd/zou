// node:timers/promises, a sleep with a value on the end of it.

export function setTimeout(delay, value, options = {}) {
  return new Promise((resolve, reject) => {
    const id = globalThis.setTimeout(() => resolve(value), delay);
    // An abort while sleeping rejects and stops the timer, so a
    // request that went away does not keep one running.
    options.signal?.addEventListener("abort", () => {
      globalThis.clearTimeout(id);
      reject(options.signal.reason);
    });
  });
}

export function setImmediate(value, options = {}) {
  return setTimeout(0, value, options);
}

export async function* setInterval(delay, value, options = {}) {
  for (;;) {
    await setTimeout(delay, undefined, options);
    yield value;
  }
}

export const scheduler = {
  wait(delay, options) {
    return setTimeout(delay, undefined, options);
  },
  yield() {
    return setTimeout(0);
  },
};

export default { setTimeout, setImmediate, setInterval, scheduler };
