// node:util/types, the predicates that answer what something is when
// `typeof` says object and `instanceof` is not safe to ask.
//
// Node's own are written against the internal representation, so they
// are right across a realm boundary. These ask the prototype's tag,
// which is right for everything a package uses them on here: one
// isolate, one realm.

function tagged(value, name) {
  return Object.prototype.toString.call(value) === `[object ${name}]`;
}

export function isDate(value) {
  return tagged(value, "Date");
}

export function isRegExp(value) {
  return tagged(value, "RegExp");
}

export function isMap(value) {
  return tagged(value, "Map");
}

export function isSet(value) {
  return tagged(value, "Set");
}

export function isPromise(value) {
  return tagged(value, "Promise");
}

export function isTypedArray(value) {
  return ArrayBuffer.isView(value) && !(value instanceof DataView);
}

export function isUint8Array(value) {
  return tagged(value, "Uint8Array");
}

export function isArrayBuffer(value) {
  return tagged(value, "ArrayBuffer");
}

export function isDataView(value) {
  return value instanceof DataView;
}

export function isArrayBufferView(value) {
  return ArrayBuffer.isView(value);
}

export function isAsyncFunction(value) {
  return tagged(value, "AsyncFunction");
}

export function isGeneratorFunction(value) {
  return tagged(value, "GeneratorFunction");
}

export function isNativeError(value) {
  return value instanceof Error;
}

export function isBoxedPrimitive(value) {
  return (
    tagged(value, "String") ||
    tagged(value, "Number") ||
    tagged(value, "Boolean") ||
    tagged(value, "Symbol") ||
    tagged(value, "BigInt")
  );
}

export function isProxy() {
  // Not answerable from javascript, and false is the answer that makes
  // a caller treat the value as itself.
  return false;
}

export default {
  isDate,
  isRegExp,
  isMap,
  isSet,
  isPromise,
  isTypedArray,
  isUint8Array,
  isArrayBuffer,
  isDataView,
  isArrayBufferView,
  isAsyncFunction,
  isGeneratorFunction,
  isNativeError,
  isBoxedPrimitive,
  isProxy,
};
