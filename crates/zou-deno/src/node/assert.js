// node:assert. A package ships assertions in its own invariants and a
// runtime without this module is a package that will not load, so what
// matters here is that a passing assertion is silent and a failing one
// throws an `AssertionError` with the fields node puts on it.

import { isDeepStrictEqual, inspect } from "node:util";

class AssertionError extends Error {
  constructor(options) {
    super(
      options.message ??
        `${inspect(options.actual)} ${options.operator} ${inspect(options.expected)}`,
    );
    this.name = "AssertionError";
    this.code = "ERR_ASSERTION";
    this.actual = options.actual;
    this.expected = options.expected;
    this.operator = options.operator;
    this.generatedMessage = options.message === undefined;
  }
}

function ok(value, message) {
  if (!value) {
    throw new AssertionError({
      message,
      actual: value,
      expected: true,
      operator: "==",
    });
  }
}

function equal(actual, expected, message) {
  // Loose on purpose: `assert.equal` is the double equals one and
  // `strictEqual` is the other.
  if (actual != expected) {
    throw new AssertionError({ message, actual, expected, operator: "==" });
  }
}

function notEqual(actual, expected, message) {
  if (actual == expected) {
    throw new AssertionError({ message, actual, expected, operator: "!=" });
  }
}

function strictEqual(actual, expected, message) {
  if (!Object.is(actual, expected)) {
    throw new AssertionError({ message, actual, expected, operator: "strictEqual" });
  }
}

function notStrictEqual(actual, expected, message) {
  if (Object.is(actual, expected)) {
    throw new AssertionError({ message, actual, expected, operator: "notStrictEqual" });
  }
}

function deepStrictEqual(actual, expected, message) {
  if (!isDeepStrictEqual(actual, expected)) {
    throw new AssertionError({ message, actual, expected, operator: "deepStrictEqual" });
  }
}

function notDeepStrictEqual(actual, expected, message) {
  if (isDeepStrictEqual(actual, expected)) {
    throw new AssertionError({ message, actual, expected, operator: "notDeepStrictEqual" });
  }
}

function fail(message) {
  throw new AssertionError({
    message: message ?? "Failed",
    operator: "fail",
  });
}

function throws(fn, expected, message) {
  try {
    fn();
  } catch (thrown) {
    if (matches(thrown, expected)) {
      return;
    }
    throw thrown;
  }
  throw new AssertionError({ message: message ?? "Missing expected exception.", operator: "throws" });
}

function doesNotThrow(fn, message) {
  try {
    fn();
  } catch (thrown) {
    throw new AssertionError({
      message: message ?? `Got unwanted exception: ${thrown}`,
      operator: "doesNotThrow",
    });
  }
}

async function rejects(work, expected, message) {
  try {
    await (typeof work === "function" ? work() : work);
  } catch (thrown) {
    if (matches(thrown, expected)) {
      return;
    }
    throw thrown;
  }
  throw new AssertionError({
    message: message ?? "Missing expected rejection.",
    operator: "rejects",
  });
}

async function doesNotReject(work, message) {
  try {
    await (typeof work === "function" ? work() : work);
  } catch (thrown) {
    throw new AssertionError({
      message: message ?? `Got unwanted rejection: ${thrown}`,
      operator: "doesNotReject",
    });
  }
}

function match(text, pattern, message) {
  if (!pattern.test(text)) {
    throw new AssertionError({
      message,
      actual: text,
      expected: pattern,
      operator: "match",
    });
  }
}

function doesNotMatch(text, pattern, message) {
  if (pattern.test(text)) {
    throw new AssertionError({
      message,
      actual: text,
      expected: pattern,
      operator: "doesNotMatch",
    });
  }
}

function ifError(value) {
  if (value !== null && value !== undefined) {
    throw new AssertionError({
      message: `ifError got unwanted exception: ${value?.message ?? value}`,
      actual: value,
      expected: null,
      operator: "ifError",
    });
  }
}

/// The four ways node lets a caller say which error was expected.
function matches(thrown, expected) {
  if (expected === undefined || typeof expected === "string") {
    return true;
  }
  if (expected instanceof RegExp) {
    return expected.test(String(thrown?.message ?? thrown));
  }
  if (typeof expected === "function") {
    return thrown instanceof expected || expected(thrown) === true;
  }
  if (typeof expected === "object" && expected !== null) {
    return Object.entries(expected).every(([key, value]) => isDeepStrictEqual(thrown[key], value));
  }
  return true;
}

// The module is the `ok` function with everything else on it, which is
// what `assert(value)` at a call site is.
const assert = Object.assign(ok, {
  AssertionError,
  ok,
  equal,
  notEqual,
  strictEqual,
  notStrictEqual,
  deepEqual: deepStrictEqual,
  notDeepEqual: notDeepStrictEqual,
  deepStrictEqual,
  notDeepStrictEqual,
  fail,
  throws,
  doesNotThrow,
  rejects,
  doesNotReject,
  match,
  doesNotMatch,
  ifError,
});
assert.strict = assert;

export default assert;
export {
  AssertionError,
  ok,
  equal,
  notEqual,
  strictEqual,
  notStrictEqual,
  deepStrictEqual,
  deepStrictEqual as deepEqual,
  notDeepStrictEqual,
  notDeepStrictEqual as notDeepEqual,
  fail,
  throws,
  doesNotThrow,
  rejects,
  doesNotReject,
  match,
  doesNotMatch,
  ifError,
};
export const strict = assert;
