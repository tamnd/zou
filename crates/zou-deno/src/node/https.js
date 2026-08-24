// node:https, which is node:http with the other default in front of a
// url that did not say which it wanted.
//
// The two are one module underneath, because the difference between
// them in node is a socket with tls on it and the socket here belongs
// to the host's http client either way.

import http, { Agent, ClientRequest, IncomingMessage, Server, STATUS_CODES, METHODS, createServer as _createServer } from "node:http";

export function request(first, second, third) {
  return http.request(...asHttps(first, second, third));
}

export function get(first, second, third) {
  return http.get(...asHttps(first, second, third));
}

/// The options, with `https:` where node's `http:` default would have
/// gone. A url that already says which protocol it is keeps it.
function asHttps(first, second, third) {
  const args = [first, second, third];
  let touched = false;
  const made = args.map((arg) => {
    if (arg !== null && typeof arg === "object" && !(arg instanceof URL) && typeof arg !== "function") {
      touched = true;
      return { protocol: "https:", ...arg };
    }
    return arg;
  });
  if (!touched && typeof first === "string" && !/^[a-z][a-z0-9+.-]*:/i.test(first)) {
    made[0] = `https://${first}`;
  }
  return made;
}

export function createServer(options, handler) {
  return _createServer(options, handler);
}

export { Agent, ClientRequest, IncomingMessage, Server, STATUS_CODES, METHODS };

export const globalAgent = new Agent({ keepAlive: true });

export default {
  Agent,
  ClientRequest,
  IncomingMessage,
  Server,
  STATUS_CODES,
  METHODS,
  createServer,
  get,
  globalAgent,
  request,
};
