// node:async_hooks, which here is one thing: a store that follows a
// value through a promise chain instead of through a call.
//
// This is the built in that is a runtime feature rather than a
// translation. A tracer wants the request it is inside of to still be
// findable after three awaits, on that chain and not on whoever else
// the event loop ran in between, and no amount of javascript can do
// that on its own: the value has to be carried by whatever resumes a
// continuation. v8 carries one, deno_core hands it to the isolate as
// `getAsyncContext` and `setAsyncContext`, and every storage here is a
// key into the one object those two move around.
//
// The context is replaced and never mutated. A `run` builds the map it
// wants, sets it, and puts the old one back when the call returns, so
// a store written inside a chain is invisible to the chain beside it
// and a chain that started earlier keeps what it started with.
//
// The hooks half of node's module is a different thing and is not
// here. `createHook` gives back a hook whose callbacks never fire,
// because nothing in this runtime reports the lifetime of a promise,
// and the ids are per resource rather than node's own numbering.

const core = Deno.core;

/// The id of the resource a chain is running inside, kept in the same
/// map the stores are, because it has to travel the same way.
const ID = Symbol("asyncId");

let counted = 1;

function current() {
  const held = core.getAsyncContext();
  return held instanceof Map ? held : null;
}

/// The context a chain should carry next: whatever it carries now,
/// with one key set or removed, as a new map.
function with_(key, value, drop) {
  const next = new Map(current() ?? []);
  if (drop) next.delete(key);
  else next.set(key, value);
  return next;
}

export class AsyncLocalStorage {
  #key = Symbol("store");
  #on = true;

  run(store, callback, ...args) {
    const held = core.getAsyncContext();
    core.setAsyncContext(with_(this.#key, store, false));
    try {
      return Reflect.apply(callback, null, args);
    } finally {
      core.setAsyncContext(held);
    }
  }

  /// The callback, run outside whatever store this one is holding,
  /// which is what a piece of work that belongs to nobody's request
  /// wants around it.
  exit(callback, ...args) {
    const held = core.getAsyncContext();
    core.setAsyncContext(with_(this.#key, undefined, true));
    try {
      return Reflect.apply(callback, null, args);
    } finally {
      core.setAsyncContext(held);
    }
  }

  getStore() {
    if (!this.#on) return undefined;
    const held = current();
    return held === null ? undefined : held.get(this.#key);
  }

  /// The store for the rest of this chain, without a callback around
  /// it. Node's own warning applies and is worth repeating: what
  /// "the rest of this chain" means is whatever resumes next, so a
  /// caller that cannot say where its own chain ends should use
  /// `run`.
  enterWith(store) {
    this.#on = true;
    core.setAsyncContext(with_(this.#key, store, false));
  }

  disable() {
    this.#on = false;
    core.setAsyncContext(with_(this.#key, undefined, true));
  }

  /// A function that runs in whatever context this line is in, however
  /// long from now it is called. This is what a callback handed to
  /// something outside the chain needs.
  static bind(callback) {
    const held = core.getAsyncContext();
    return function bound(...args) {
      const mine = core.getAsyncContext();
      core.setAsyncContext(held);
      try {
        return Reflect.apply(callback, this, args);
      } finally {
        core.setAsyncContext(mine);
      }
    };
  }

  static snapshot() {
    const held = core.getAsyncContext();
    return function within(callback, ...args) {
      const mine = core.getAsyncContext();
      core.setAsyncContext(held);
      try {
        return Reflect.apply(callback, null, args);
      } finally {
        core.setAsyncContext(mine);
      }
    };
  }
}

/// A named thing a chain can be inside of, which for everything here
/// is an id and a way to run a callback in the context the resource
/// was made in. Node's version is a handle on a libuv request, and a
/// package uses it for exactly this.
export class AsyncResource {
  #held;
  #id;
  #trigger;

  constructor(type, options = {}) {
    this.type = String(type);
    this.#held = core.getAsyncContext();
    this.#id = ++counted;
    this.#trigger = options.triggerAsyncId ?? executionAsyncId();
  }

  runInAsyncScope(callback, thisArg, ...args) {
    const mine = core.getAsyncContext();
    const inside = new Map(this.#held instanceof Map ? this.#held : []);
    inside.set(ID, this.#id);
    core.setAsyncContext(inside);
    try {
      return Reflect.apply(callback, thisArg, args);
    } finally {
      core.setAsyncContext(mine);
    }
  }

  bind(callback, thisArg) {
    const resource = this;
    return function bound(...args) {
      return resource.runInAsyncScope(callback, thisArg ?? this, ...args);
    };
  }

  asyncId() {
    return this.#id;
  }

  triggerAsyncId() {
    return this.#trigger;
  }

  emitDestroy() {
    return this;
  }

  static bind(callback, type, thisArg) {
    return new AsyncResource(type ?? callback.name ?? "bound-anonymous-fn").bind(callback, thisArg);
  }
}

export function executionAsyncId() {
  const held = current();
  const id = held === null ? undefined : held.get(ID);
  // Node's 1 is the top level, which is where a chain that has never
  // been inside a resource is.
  return id ?? 1;
}

export function triggerAsyncId() {
  return 0;
}

export function executionAsyncResource() {
  return {};
}

/// The hooks, which never fire. Nothing in this runtime reports when
/// a promise is made, resolved or collected, so a hook that says it
/// is enabled and then calls nothing is the honest shape: a tracer
/// that installs one keeps working, and it sees the chain through
/// `AsyncLocalStorage`, which does work.
export function createHook() {
  return {
    enable() {
      return this;
    },
    disable() {
      return this;
    },
  };
}

export const asyncWrapProviders = Object.freeze({ NONE: 0 });

export default {
  AsyncLocalStorage,
  AsyncResource,
  createHook,
  executionAsyncId,
  executionAsyncResource,
  triggerAsyncId,
  asyncWrapProviders,
};
