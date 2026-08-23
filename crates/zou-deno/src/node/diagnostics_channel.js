// node:diagnostics_channel. A named list of subscribers and a publish
// that walks it, which is the whole of the module: nothing here is
// asynchronous and nothing needs the host.
//
// It is here because instrumentation libraries import it to find out
// whether anybody is listening, and on a function nobody is. Sentry
// imports it and publishes into channels no subscriber of this
// runtime's ever joins, so the cost of having it is a map lookup per
// publish and the benefit is that the package imports at all. See
// #593.
//
// The one thing worth getting right is `hasSubscribers`, since that is
// what a library checks before it builds a message it is about to
// throw away.

/// Every channel that has ever been named, so that two callers asking
/// for one name get the same object, which is what makes a subscriber
/// registered before a publisher hears the publish.
const named = new Map();

export class Channel {
  #subscribers = [];
  #store = null;
  #transform = null;

  constructor(name) {
    this.name = name;
  }

  get hasSubscribers() {
    return this.#subscribers.length > 0;
  }

  subscribe(handler) {
    if (typeof handler !== "function") {
      throw new TypeError("a channel subscriber must be a function");
    }
    this.#subscribers.push(handler);
  }

  unsubscribe(handler) {
    const at = this.#subscribers.indexOf(handler);
    if (at === -1) {
      return false;
    }
    this.#subscribers.splice(at, 1);
    return true;
  }

  /// A subscriber that throws does not stop the ones after it and does
  /// not reach the publisher, which is node's behaviour and the only
  /// one that makes instrumentation safe to add: a broken listener
  /// must not break the thing it is listening to. Node reports it as
  /// an uncaught exception, and here it becomes an unhandled rejection
  /// through a promise nobody holds, which lands in the same place.
  publish(message) {
    for (const handler of this.#subscribers.slice()) {
      try {
        handler(message, this.name);
      } catch (e) {
        Promise.reject(e);
      }
    }
  }

  bindStore(store, transform) {
    this.#store = store;
    this.#transform = transform ?? null;
  }

  unbindStore(store) {
    if (this.#store !== store) {
      return false;
    }
    this.#store = null;
    this.#transform = null;
    return true;
  }

  /// There is no async local storage here, so a bound store is
  /// remembered and the body is simply run. A caller reading the store
  /// inside sees whatever it saw outside, which is the honest answer
  /// for a runtime with no context to propagate.
  runStores(message, fn, thisArg, ...args) {
    this.publish(message);
    return fn.apply(thisArg, args);
  }
}

export function channel(name) {
  let found = named.get(name);
  if (found === undefined) {
    found = new Channel(name);
    named.set(name, found);
  }
  return found;
}

export function hasSubscribers(name) {
  const found = named.get(name);
  return found !== undefined && found.hasSubscribers;
}

export function subscribe(name, handler) {
  channel(name).subscribe(handler);
}

export function unsubscribe(name, handler) {
  return channel(name).unsubscribe(handler);
}

/// The five channels a traced call publishes to, under one name.
///
/// A library subscribes to a tracing channel with an object of
/// handlers rather than to each of the five, which is the only reason
/// this wrapper exists rather than the caller naming `name:start` and
/// the rest itself.
export class TracingChannel {
  constructor(name) {
    const of = (part) => (typeof name === "string" ? channel(`tracing:${name}:${part}`) : name[part]);
    this.start = of("start");
    this.end = of("end");
    this.asyncStart = of("asyncStart");
    this.asyncEnd = of("asyncEnd");
    this.error = of("error");
  }

  get hasSubscribers() {
    return (
      this.start.hasSubscribers ||
      this.end.hasSubscribers ||
      this.asyncStart.hasSubscribers ||
      this.asyncEnd.hasSubscribers ||
      this.error.hasSubscribers
    );
  }

  subscribe(handlers) {
    for (const part of ["start", "end", "asyncStart", "asyncEnd", "error"]) {
      if (handlers[part] !== undefined) {
        this[part].subscribe(handlers[part]);
      }
    }
  }

  unsubscribe(handlers) {
    let all = true;
    for (const part of ["start", "end", "asyncStart", "asyncEnd", "error"]) {
      if (handlers[part] !== undefined && !this[part].unsubscribe(handlers[part])) {
        all = false;
      }
    }
    return all;
  }

  traceSync(fn, context = {}, thisArg, ...args) {
    this.start.publish(context);
    try {
      const result = fn.apply(thisArg, args);
      context.result = result;
      return result;
    } catch (e) {
      context.error = e;
      this.error.publish(context);
      throw e;
    } finally {
      this.end.publish(context);
    }
  }

  tracePromise(fn, context = {}, thisArg, ...args) {
    this.start.publish(context);
    let promise;
    try {
      promise = Promise.resolve(fn.apply(thisArg, args));
    } catch (e) {
      context.error = e;
      this.error.publish(context);
      this.end.publish(context);
      throw e;
    }
    this.end.publish(context);
    this.asyncStart.publish(context);
    return promise.then(
      (result) => {
        context.result = result;
        this.asyncEnd.publish(context);
        return result;
      },
      (e) => {
        context.error = e;
        this.error.publish(context);
        this.asyncEnd.publish(context);
        throw e;
      },
    );
  }

  /// The callback flavour, where the position of the callback in the
  /// argument list is the caller's to say because node has no way of
  /// knowing it either.
  traceCallback(fn, position = -1, context = {}, thisArg, ...args) {
    const at = position < 0 ? args.length + position : position;
    const given = args[at];
    args[at] = (...answered) => {
      const [e, result] = answered;
      if (e !== null && e !== undefined) {
        context.error = e;
        this.error.publish(context);
      } else {
        context.result = result;
      }
      this.asyncStart.publish(context);
      try {
        return given?.apply(thisArg, answered);
      } finally {
        this.asyncEnd.publish(context);
      }
    };
    this.start.publish(context);
    try {
      return fn.apply(thisArg, args);
    } catch (e) {
      context.error = e;
      this.error.publish(context);
      throw e;
    } finally {
      this.end.publish(context);
    }
  }
}

export function tracingChannel(name) {
  return new TracingChannel(name);
}

export default {
  Channel,
  TracingChannel,
  channel,
  hasSubscribers,
  subscribe,
  unsubscribe,
  tracingChannel,
};
