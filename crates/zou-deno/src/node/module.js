// node:module. What a package in an ESM build wants from this is a
// `require`, and what it usually wants the require for is to ask
// whether it is on node at all: `createRequire(import.meta.url)` at
// the top, `require("node:something")` in a branch, and a fallback
// when either throws. See #593.
//
// There is no CJS here, so the require this hands back is not node's:
// it serves the built ins and refuses everything else by name. That is
// the honest shape of it. A package asking for `node:fs` through a
// require gets the same module it would have got through an import,
// and a package asking for a package gets a sentence saying this
// runtime has no require to resolve one with, which is what it would
// have got from a browser build too.
//
// Serving them synchronously is why every built in is imported here
// rather than looked up when asked: a require returns a value and
// cannot wait for a module to load, so the ones it can serve have to
// already be in the graph. The cost is that importing node:module
// brings the rest of node:* with it, which is a compile of about
// twenty small modules from memory and no network at all.

import * as assert from "node:assert";
import * as buffer from "node:buffer";
import * as child_process from "node:child_process";
import * as crypto from "node:crypto";
import * as diagnostics_channel from "node:diagnostics_channel";
import * as events from "node:events";
import * as fs from "node:fs";
import * as fs_promises from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import * as process from "node:process";
import * as querystring from "node:querystring";
import * as stream from "node:stream";
import * as stream_promises from "node:stream/promises";
import * as stream_web from "node:stream/web";
import * as string_decoder from "node:string_decoder";
import * as timers from "node:timers";
import * as timers_promises from "node:timers/promises";
import * as url from "node:url";
import * as util from "node:util";
import * as util_types from "node:util/types";

/// Every built in this runtime has, by the name it is asked for.
///
/// `module` itself is not in it, and that is deliberate rather than an
/// oversight: a require of `node:module` would be this module asking
/// for itself while it is still being made.
const BUILT_INS = {
  assert,
  buffer,
  child_process,
  crypto,
  diagnostics_channel,
  events,
  fs,
  "fs/promises": fs_promises,
  os,
  path,
  "path/posix": path,
  "path/win32": path,
  process,
  querystring,
  stream,
  "stream/promises": stream_promises,
  "stream/web": stream_web,
  string_decoder,
  timers,
  "timers/promises": timers_promises,
  url,
  util,
  "util/types": util_types,
};

/// What node calls the built in list, which is the names without the
/// prefix and sorted, because a package that prints it prints it.
export const builtinModules = Object.keys(BUILT_INS).concat("module").sort();

export function isBuiltin(name) {
  const bare = typeof name === "string" && name.startsWith("node:") ? name.slice(5) : name;
  return builtinModules.includes(bare);
}

/// The namespace a require answers with.
///
/// A module written for CJS reads `module.exports`, and a namespace
/// object is not that: the default export is where a shim puts the
/// object node's own require would have returned, so that is what
/// comes back when there is one, with the named exports over the top
/// for a caller that destructures.
function required(namespace) {
  const asDefault = namespace.default;
  if (asDefault === undefined || asDefault === null) {
    return namespace;
  }
  if (typeof asDefault === "function") {
    return asDefault;
  }
  return Object.assign(Object.create(null), asDefault, namespace);
}

export function createRequire(from) {
  if (typeof from !== "string" && !(from instanceof URL)) {
    throw new TypeError("createRequire takes a file url or a path");
  }
  const require = function require(name) {
    const bare = typeof name === "string" && name.startsWith("node:") ? name.slice(5) : name;
    const found = BUILT_INS[bare];
    if (found !== undefined) {
      return required(found);
    }
    if (bare === "module") {
      return self;
    }
    throw new Error(
      `Cannot find module '${name}'. A function has no require to resolve one with, only the node built ins are here`,
    );
  };
  require.resolve = function resolve(name) {
    if (isBuiltin(name)) {
      return typeof name === "string" && name.startsWith("node:") ? name : `node:${name}`;
    }
    throw new Error(
      `Cannot find module '${name}'. A function has no require to resolve one with, only the node built ins are here`,
    );
  };
  require.resolve.paths = () => null;
  require.cache = Object.create(null);
  require.extensions = Object.create(null);
  require.main = undefined;
  return require;
}

/// A no operation, because there is nothing to keep in step: the named
/// exports of these modules are fixed at the point the shim was
/// written and nothing can add to them at runtime.
export function syncBuiltinESMExports() {}

/// Loader hooks, which are a thing a program that owns its own process
/// installs. A function does not own one, and a hook that silently did
/// nothing would be worse than a refusal, since the whole point of
/// registering one is to change how later imports resolve.
export function register() {
  throw new TypeError("a function cannot register a module loader, node:module register is not here");
}

export const Module = {
  builtinModules,
  createRequire,
  isBuiltin,
  syncBuiltinESMExports,
  register,
  _cache: Object.create(null),
  _extensions: Object.create(null),
};

const self = {
  Module,
  builtinModules,
  createRequire,
  isBuiltin,
  syncBuiltinESMExports,
  register,
};

export default Module;
