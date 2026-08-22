// node:util. Four of these carry almost all the weight in library
// code: `promisify`, `inherits`, `format` and `inspect`. The rest of
// the module is the legacy type predicates, which are one line each
// and are still called.

import types from "node:util/types";

const custom = Symbol.for("nodejs.util.inspect.custom");
const promisifyCustom = Symbol.for("nodejs.util.promisify.custom");

/// The node callback convention turned into a promise: the last
/// argument is `(error, value)` and the value is what the promise
/// settles with.
function promisify(fn) {
  if (typeof fn !== "function") {
    const wrong = new TypeError('The "original" argument must be of type function');
    wrong.code = "ERR_INVALID_ARG_TYPE";
    throw wrong;
  }
  // A package that knows its own callback shape says so with this
  // symbol, and node uses what it says rather than wrapping.
  if (fn[promisifyCustom]) {
    return fn[promisifyCustom];
  }
  const promised = function (...args) {
    return new Promise((resolve, reject) => {
      fn.call(this, ...args, (why, ...values) => {
        if (why) {
          reject(why);
        } else {
          resolve(values.length > 1 ? values : values[0]);
        }
      });
    });
  };
  Object.setPrototypeOf(promised, Object.getPrototypeOf(fn));
  Object.defineProperty(promised, "name", { value: fn.name });
  return promised;
}

/// The other direction, for an API that takes a callback and was
/// handed something that returns a promise.
function callbackify(fn) {
  const called = function (...args) {
    const back = args.pop();
    Promise.resolve(fn.apply(this, args)).then(
      (value) => back(null, value),
      (why) => back(why ?? new Error("Promise was rejected with a falsy value")),
    );
  };
  Object.defineProperty(called, "name", { value: fn.name });
  return called;
}

/// The prototype chain node's own classes are built with. Not `extends`
/// and not a copy: the subclass keeps its own prototype object and gets
/// the superclass behind it, which is what code written before classes
/// expects.
function inherits(ctor, superCtor) {
  Object.defineProperty(ctor, "super_", {
    value: superCtor,
    writable: true,
    configurable: true,
  });
  Object.setPrototypeOf(ctor.prototype, superCtor.prototype);
}

function inspect(value, options = {}) {
  const depth = options.depth ?? 2;
  return shown(value, depth, new Set());
}

function shown(value, depth, seen) {
  if (typeof value === "string") {
    return `'${value.replaceAll("'", "\\'")}'`;
  }
  if (typeof value === "bigint") {
    return `${value}n`;
  }
  if (typeof value === "function") {
    return value.name ? `[Function: ${value.name}]` : "[Function (anonymous)]";
  }
  if (typeof value !== "object" || value === null) {
    return String(value);
  }
  if (value instanceof Error) {
    return value.stack ?? `${value.name}: ${value.message}`;
  }
  if (typeof value[custom] === "function") {
    return String(value[custom](depth, {}));
  }
  if (seen.has(value)) {
    return "[Circular *1]";
  }
  if (depth < 0) {
    return Array.isArray(value) ? "[Array]" : "[Object]";
  }
  seen.add(value);
  try {
    if (Array.isArray(value)) {
      return `[ ${value.map((it) => shown(it, depth - 1, seen)).join(", ")} ]`;
    }
    if (value instanceof Map) {
      const pairs = Array.from(
        value,
        ([key, held]) => `${shown(key, depth - 1, seen)} => ${shown(held, depth - 1, seen)}`,
      );
      return `Map(${value.size}) { ${pairs.join(", ")} }`;
    }
    if (value instanceof Set) {
      return `Set(${value.size}) { ${Array.from(value, (it) => shown(it, depth - 1, seen)).join(", ")} }`;
    }
    if (ArrayBuffer.isView(value)) {
      return `${value.constructor.name}(${value.length}) [ ${Array.from(value).join(", ")} ]`;
    }
    const name = value.constructor?.name;
    const prefix = name && name !== "Object" ? `${name} ` : "";
    const pairs = Object.entries(value).map(
      ([key, held]) => `${key}: ${shown(held, depth - 1, seen)}`,
    );
    return pairs.length === 0 ? `${prefix}{}` : `${prefix}{ ${pairs.join(", ")} }`;
  } finally {
    seen.delete(value);
  }
}

/// printf the way node does it, with the placeholders it has and the
/// rule that whatever is left over is appended.
function format(first, ...rest) {
  return formatWithOptions({}, first, ...rest);
}

function formatWithOptions(options, first, ...rest) {
  const out = [];
  let at = 0;
  if (typeof first === "string" && first.includes("%")) {
    let text = "";
    for (let index = 0; index < first.length; index += 1) {
      if (first[index] !== "%" || index + 1 === first.length) {
        text += first[index];
        continue;
      }
      const kind = first[index + 1];
      index += 1;
      if (kind === "%") {
        text += "%";
        continue;
      }
      if (at >= rest.length) {
        text += "%" + kind;
        continue;
      }
      const value = rest[at];
      switch (kind) {
        case "s":
          text += typeof value === "object" && value !== null ? inspect(value, options) : String(value);
          break;
        case "d":
        case "f":
          text += typeof value === "bigint" ? `${value}n` : Number(value);
          break;
        case "i":
          text += typeof value === "bigint" ? `${value}n` : Number.parseInt(value, 10);
          break;
        case "j":
          try {
            text += JSON.stringify(value);
          } catch {
            text += "[Circular]";
          }
          break;
        case "o":
        case "O":
          text += inspect(value, options);
          break;
        case "c":
          // A css placeholder, which has nothing to style here.
          break;
        default:
          text += "%" + kind;
          at -= 1;
          break;
      }
      at += 1;
    }
    out.push(text);
  } else if (first !== undefined) {
    out.push(typeof first === "string" ? first : inspect(first, options));
  }
  for (; at < rest.length; at += 1) {
    const value = rest[at];
    out.push(typeof value === "string" ? value : inspect(value, options));
  }
  return out.join(" ");
}

/// A wrapper that says the thing is going away, once per call site, on
/// the first call. Node says it once per process and so does this.
function deprecate(fn, message) {
  let said = false;
  return function (...args) {
    if (!said) {
      said = true;
      console.warn(message);
    }
    return fn.apply(this, args);
  };
}

/// `util.debuglog` is a logger a program turns on with NODE_DEBUG, and
/// nothing here turns it on, so it is a function that does nothing and
/// says it is disabled when asked.
function debuglog() {
  const off = () => {};
  off.enabled = false;
  return off;
}

function isDeepStrictEqual(one, two) {
  return same(one, two);
}

function same(one, two) {
  if (Object.is(one, two)) {
    return true;
  }
  if (typeof one !== "object" || typeof two !== "object" || one === null || two === null) {
    return false;
  }
  if (Object.getPrototypeOf(one) !== Object.getPrototypeOf(two)) {
    return false;
  }
  if (one instanceof Date) {
    return one.getTime() === two.getTime();
  }
  if (one instanceof RegExp) {
    return String(one) === String(two);
  }
  if (ArrayBuffer.isView(one)) {
    return one.length === two.length && Array.prototype.every.call(one, (it, at) => it === two[at]);
  }
  if (one instanceof Map) {
    return (
      one.size === two.size &&
      Array.from(one).every(([key, held]) => two.has(key) && same(held, two.get(key)))
    );
  }
  if (one instanceof Set) {
    return one.size === two.size && Array.from(one).every((it) => two.has(it));
  }
  const keys = Reflect.ownKeys(one);
  return (
    keys.length === Reflect.ownKeys(two).length && keys.every((key) => same(one[key], two[key]))
  );
}

const TextEncoderClass = globalThis.TextEncoder;
const TextDecoderClass = globalThis.TextDecoder;

// The predicates node kept from before there was a `types` namespace.
const isArray = Array.isArray;
const isDate = types.isDate;
const isRegExp = types.isRegExp;
const isError = (value) => value instanceof Error;
const isFunction = (value) => typeof value === "function";
const isString = (value) => typeof value === "string";
const isNumber = (value) => typeof value === "number";
const isBoolean = (value) => typeof value === "boolean";
const isNull = (value) => value === null;
const isUndefined = (value) => value === undefined;
const isNullOrUndefined = (value) => value === null || value === undefined;
const isObject = (value) => typeof value === "object" && value !== null;
const isPrimitive = (value) => value === null || (typeof value !== "object" && typeof value !== "function");
const isBuffer = (value) => ArrayBuffer.isView(value);

function toUSVString(value) {
  return String(value).replaceAll(/[\uD800-\uDFFF]/gu, "�");
}

/// `util.parseArgs` and `util.stripVTControlCharacters` are the two a
/// command line program wants, and a function is not one. The first is
/// missing by name and the second is a regular expression.
function stripVTControlCharacters(text) {
  return String(text).replaceAll(/\[[0-9;]*[A-Za-z]/g, "");
}

const util = {
  promisify,
  callbackify,
  inherits,
  inspect,
  format,
  formatWithOptions,
  deprecate,
  debuglog,
  debug: debuglog,
  isDeepStrictEqual,
  types,
  TextEncoder: TextEncoderClass,
  TextDecoder: TextDecoderClass,
  toUSVString,
  stripVTControlCharacters,
  isArray,
  isDate,
  isRegExp,
  isError,
  isFunction,
  isString,
  isNumber,
  isBoolean,
  isNull,
  isUndefined,
  isNullOrUndefined,
  isObject,
  isPrimitive,
  isBuffer,
};
inspect.custom = custom;
promisify.custom = promisifyCustom;

export default util;
export {
  promisify,
  callbackify,
  inherits,
  inspect,
  format,
  formatWithOptions,
  deprecate,
  debuglog,
  debuglog as debug,
  isDeepStrictEqual,
  types,
  TextEncoderClass as TextEncoder,
  TextDecoderClass as TextDecoder,
  toUSVString,
  stripVTControlCharacters,
  isArray,
  isDate,
  isRegExp,
  isError,
  isFunction,
  isString,
  isNumber,
  isBoolean,
  isNull,
  isUndefined,
  isNullOrUndefined,
  isObject,
  isPrimitive,
  isBuffer,
};
