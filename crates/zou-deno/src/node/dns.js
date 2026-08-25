// node:dns, which is node:dns/promises with a callback on the end.
//
// The queries are all in the promises module, since that is where the
// work is and a second copy of it would be a second set of shapes to
// keep true. What is here is node's older calling convention, which is
// still what most packages use: the answer arrives as an argument to a
// function rather than as a value, and a failure arrives there too
// rather than being thrown.
//
// `lookup` is the one that does not simply hand its answer over. Node
// calls back with the address and the family as two arguments, and only
// with a list when it was asked for all of them.

import promises from "node:dns/promises";
import { Resolver as Promised } from "node:dns/promises";

/// A promise, answered the way node answers: the failure first, and
/// nothing else with it.
function answering(promise, back) {
  if (typeof back !== "function") {
    throw new TypeError("node:dns takes a callback, or use node:dns/promises");
  }
  promise.then((answer) => back(null, answer), (why) => back(why));
}

export function lookup(hostname, options, back) {
  if (typeof options === "function") {
    back = options;
    options = {};
  }
  if (typeof back !== "function") {
    throw new TypeError("node:dns lookup takes a callback, or use node:dns/promises");
  }
  const all = options !== null && typeof options === "object" && options.all === true;
  promises.lookup(hostname, options).then(
    (found) => (all ? back(null, found) : back(null, found.address, found.family)),
    (why) => back(why),
  );
}

export function lookupService(address, port, back) {
  answering(promises.lookupService(address, port), back);
}

export function resolve(hostname, rrtype, back) {
  if (typeof rrtype === "function") {
    back = rrtype;
    rrtype = "A";
  }
  answering(promises.resolve(hostname, rrtype), back);
}

export function resolve4(hostname, options, back) {
  if (typeof options === "function") {
    back = options;
    options = {};
  }
  answering(promises.resolve4(hostname, options), back);
}

export function resolve6(hostname, options, back) {
  if (typeof options === "function") {
    back = options;
    options = {};
  }
  answering(promises.resolve6(hostname, options), back);
}

export function resolveCname(hostname, back) {
  answering(promises.resolveCname(hostname), back);
}

export function resolveNs(hostname, back) {
  answering(promises.resolveNs(hostname), back);
}

export function resolvePtr(hostname, back) {
  answering(promises.resolvePtr(hostname), back);
}

export function resolveTxt(hostname, back) {
  answering(promises.resolveTxt(hostname), back);
}

export function resolveMx(hostname, back) {
  answering(promises.resolveMx(hostname), back);
}

export function resolveSrv(hostname, back) {
  answering(promises.resolveSrv(hostname), back);
}

export function resolveSoa(hostname, back) {
  answering(promises.resolveSoa(hostname), back);
}

export function resolveCaa(hostname, back) {
  answering(promises.resolveCaa(hostname), back);
}

export function resolveNaptr(hostname, back) {
  answering(promises.resolveNaptr(hostname), back);
}

export function resolveAny(hostname, back) {
  answering(promises.resolveAny(hostname), back);
}

export function reverse(address, back) {
  answering(promises.reverse(address), back);
}

export const getServers = promises.getServers;
export const setServers = promises.setServers;

/// Which of two addresses for the same name comes first. There is one
/// order here, the one the host answered in, and nothing to set it to.
export function setDefaultResultOrder() {}

export function getDefaultResultOrder() {
  return "verbatim";
}

/// The same resolver, asked in the older way.
export class Resolver {
  #resolver = new Promised();

  constructor() {
    this.cancel = () => {};
    this.setLocalAddress = () => {};
  }

  getServers() {
    return this.#resolver.getServers();
  }

  setServers(list) {
    this.#resolver.setServers(list);
  }

  resolve(hostname, rrtype, back) {
    if (typeof rrtype === "function") {
      back = rrtype;
      rrtype = "A";
    }
    answering(this.#resolver.resolve(hostname, rrtype), back);
  }

  resolve4(hostname, back) {
    answering(this.#resolver.resolve4(hostname), back);
  }

  resolve6(hostname, back) {
    answering(this.#resolver.resolve6(hostname), back);
  }

  resolveCname(hostname, back) {
    answering(this.#resolver.resolveCname(hostname), back);
  }

  resolveNs(hostname, back) {
    answering(this.#resolver.resolveNs(hostname), back);
  }

  resolvePtr(hostname, back) {
    answering(this.#resolver.resolvePtr(hostname), back);
  }

  resolveTxt(hostname, back) {
    answering(this.#resolver.resolveTxt(hostname), back);
  }

  resolveMx(hostname, back) {
    answering(this.#resolver.resolveMx(hostname), back);
  }

  resolveSrv(hostname, back) {
    answering(this.#resolver.resolveSrv(hostname), back);
  }

  resolveSoa(hostname, back) {
    answering(this.#resolver.resolveSoa(hostname), back);
  }

  resolveCaa(hostname, back) {
    answering(this.#resolver.resolveCaa(hostname), back);
  }

  resolveAny(hostname, back) {
    answering(this.#resolver.resolveAny(hostname), back);
  }

  resolveNaptr(hostname, back) {
    answering(this.#resolver.resolveNaptr(hostname), back);
  }

  reverse(address, back) {
    answering(this.#resolver.reverse(address), back);
  }
}

export const ADDRCONFIG = promises.ADDRCONFIG;
export const V4MAPPED = promises.V4MAPPED;
export const ALL = promises.ALL;

export const NODATA = promises.NODATA;
export const FORMERR = promises.FORMERR;
export const SERVFAIL = promises.SERVFAIL;
export const NOTFOUND = promises.NOTFOUND;
export const NOTIMP = promises.NOTIMP;
export const REFUSED = promises.REFUSED;
export const BADQUERY = promises.BADQUERY;
export const BADNAME = promises.BADNAME;
export const BADFAMILY = promises.BADFAMILY;
export const BADRESP = promises.BADRESP;
export const CONNREFUSED = promises.CONNREFUSED;
export const TIMEOUT = promises.TIMEOUT;
export const CANCELLED = promises.CANCELLED;

export { promises };

export default {
  ADDRCONFIG,
  ALL,
  BADFAMILY,
  BADNAME,
  BADQUERY,
  BADRESP,
  CANCELLED,
  CONNREFUSED,
  FORMERR,
  NODATA,
  NOTFOUND,
  NOTIMP,
  REFUSED,
  Resolver,
  SERVFAIL,
  TIMEOUT,
  V4MAPPED,
  getDefaultResultOrder,
  getServers,
  lookup,
  lookupService,
  promises,
  resolve,
  resolve4,
  resolve6,
  resolveAny,
  resolveCaa,
  resolveCname,
  resolveMx,
  resolveNaptr,
  resolveNs,
  resolvePtr,
  resolveSoa,
  resolveSrv,
  resolveTxt,
  reverse,
  setDefaultResultOrder,
  setServers,
};
