// node:worker_threads. A function is one isolate and cannot start
// another, so a `Worker` is the thing that cannot work here. Most of
// the rest of the module can, and says something true: this is the
// main thread, there is no port back to a parent, and there is no
// data somebody started it with.
//
// Here for the same reason node:child_process is: a package that
// imports it at the top and starts a worker only when asked runs
// without ever starting one, and an import that is refused by name
// stops it before it begins. See #593.
//
// The channel half is the platform's, since the prelude already has a
// MessageChannel and a MessagePort and node's are the same two things
// under the same names.

export const isMainThread = true;
export const isInternalThread = false;
export const parentPort = null;
export const workerData = null;
export const threadId = 0;
export const resourceLimits = {};
export const SHARE_ENV = Symbol.for("nodejs.worker_threads.SHARE_ENV");

export const MessageChannel = globalThis.MessageChannel;
export const MessagePort = globalThis.MessagePort;
export const BroadcastChannel = globalThis.BroadcastChannel;

export class Worker {
  constructor() {
    throw new TypeError(
      "a function is one isolate and cannot start another, so node:worker_threads Worker cannot work here",
    );
  }
}

/// A port with nothing queued on it, which is what this always is:
/// nothing here delivers a message without the event loop, so a
/// synchronous read of one never has anything to find.
export function receiveMessageOnPort() {
  return undefined;
}

export function markAsUntransferable() {}

export function isMarkedAsUntransferable() {
  return false;
}

export function moveMessagePortToContext() {
  throw new TypeError("a function has one context, so node:worker_threads moveMessagePortToContext cannot work here");
}

/// The data a program hands its workers. There are no workers, so
/// setting it is remembered and reading it back is the only thing that
/// ever observes it.
const environment = new Map();

export function setEnvironmentData(key, value) {
  if (value === undefined) {
    environment.delete(key);
    return;
  }
  environment.set(key, value);
}

export function getEnvironmentData(key) {
  return environment.get(key);
}

export default {
  isMainThread,
  isInternalThread,
  parentPort,
  workerData,
  threadId,
  resourceLimits,
  SHARE_ENV,
  MessageChannel,
  MessagePort,
  BroadcastChannel,
  Worker,
  receiveMessageOnPort,
  markAsUntransferable,
  isMarkedAsUntransferable,
  moveMessagePortToContext,
  setEnvironmentData,
  getEnvironmentData,
};
