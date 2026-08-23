// node:cluster. A server that forks itself once per core is a shape a
// function is on the other side of: the forking is the host's job and
// happened long before any of this ran.
//
// So what is true is said, and the two calls that would fork throw.
// Here at all for the reason node:child_process is: a package that
// imports this and only calls it when it is the one running the
// program never calls it here. See #593.

export const isPrimary = true;
export const isMaster = true;
export const isWorker = false;
export const worker = undefined;
export const workers = Object.create(null);
export const settings = {};
export const SCHED_NONE = 1;
export const SCHED_RR = 2;
export const schedulingPolicy = SCHED_NONE;

function noForking(name) {
  return function () {
    throw new TypeError(`a function does not run the server it is on, so node:cluster ${name} cannot work here`);
  };
}

export const fork = noForking("fork");
export const setupPrimary = noForking("setupPrimary");
export const setupMaster = noForking("setupMaster");
export const disconnect = noForking("disconnect");

/// The event side of it, which costs nothing to have and answers the
/// packages that attach a listener at the top of themselves. Nothing
/// ever emits on it, because nothing here forks.
export function on() {
  return cluster;
}

export const once = on;
export const off = () => cluster;
export const addListener = on;
export const removeListener = () => cluster;
export const removeAllListeners = () => cluster;
export const emit = () => false;
export const listenerCount = () => 0;
export const listeners = () => [];
export const eventNames = () => [];

const cluster = {
  isPrimary,
  isMaster,
  isWorker,
  worker,
  workers,
  settings,
  SCHED_NONE,
  SCHED_RR,
  schedulingPolicy,
  fork,
  setupPrimary,
  setupMaster,
  disconnect,
  on,
  once,
  off,
  addListener,
  removeListener,
  removeAllListeners,
  emit,
  listenerCount,
  listeners,
  eventNames,
};

export default cluster;
