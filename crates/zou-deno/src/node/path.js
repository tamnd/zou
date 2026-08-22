// node:path, posix, which is the only kind of path a function on this
// runtime can be handed: the host is a unix filesystem and a project's
// files were laid out by the deploy.
//
// `path.win32` is here and is the same code, because a package that
// asks for it on a posix machine is asking for a namespace to exist
// rather than for backslashes, and node ships both on both.

function assertPath(path) {
  if (typeof path !== "string") {
    const wrong = new TypeError(
      `The "path" argument must be of type string. Received ${typeof path}`,
    );
    wrong.code = "ERR_INVALID_ARG_TYPE";
    throw wrong;
  }
}

const sep = "/";
const delimiter = ":";

/// The parts of a path with `.` dropped and `..` applied, which is the
/// whole of what normalising one is.
function walked(path, allowAboveRoot) {
  const out = [];
  for (const part of path.split("/")) {
    if (part.length === 0 || part === ".") {
      continue;
    }
    if (part !== "..") {
      out.push(part);
      continue;
    }
    if (out.length > 0 && out[out.length - 1] !== "..") {
      out.pop();
    } else if (allowAboveRoot) {
      out.push("..");
    }
  }
  return out.join("/");
}

function normalize(path) {
  assertPath(path);
  if (path.length === 0) {
    return ".";
  }
  const absolute = path.charCodeAt(0) === 47;
  const trailing = path.charCodeAt(path.length - 1) === 47;
  let walk = walked(path, !absolute);
  if (walk.length === 0) {
    if (absolute) {
      return "/";
    }
    return trailing ? "./" : ".";
  }
  if (trailing) {
    walk += "/";
  }
  return absolute ? "/" + walk : walk;
}

function isAbsolute(path) {
  assertPath(path);
  return path.length > 0 && path.charCodeAt(0) === 47;
}

function join(...parts) {
  if (parts.length === 0) {
    return ".";
  }
  let joined;
  for (const part of parts) {
    assertPath(part);
    if (part.length === 0) {
      continue;
    }
    joined = joined === undefined ? part : joined + "/" + part;
  }
  return joined === undefined ? "." : normalize(joined);
}

function resolve(...parts) {
  let resolved = "";
  let absolute = false;
  for (let at = parts.length - 1; at >= 0 && !absolute; at -= 1) {
    const part = parts[at];
    assertPath(part);
    if (part.length === 0) {
      continue;
    }
    resolved = resolved.length === 0 ? part : part + "/" + resolved;
    absolute = part.charCodeAt(0) === 47;
  }
  if (!absolute) {
    // The working directory, which is the process's, the same as node.
    const here = globalThis.Deno?.cwd?.() ?? "/";
    resolved = resolved.length === 0 ? here : here + "/" + resolved;
  }
  const walk = walked(resolved, false);
  return walk.length === 0 ? "/" : "/" + walk;
}

function relative(from, to) {
  assertPath(from);
  assertPath(to);
  if (from === to) {
    return "";
  }
  const one = resolve(from).split("/").filter(Boolean);
  const two = resolve(to).split("/").filter(Boolean);
  let same = 0;
  while (same < one.length && same < two.length && one[same] === two[same]) {
    same += 1;
  }
  const up = new Array(one.length - same).fill("..");
  return [...up, ...two.slice(same)].join("/");
}

function dirname(path) {
  assertPath(path);
  if (path.length === 0) {
    return ".";
  }
  const absolute = path.charCodeAt(0) === 47;
  let end = -1;
  let seen = false;
  for (let at = path.length - 1; at >= 1; at -= 1) {
    if (path.charCodeAt(at) === 47) {
      if (seen) {
        end = at;
        break;
      }
    } else {
      seen = true;
    }
  }
  if (end === -1) {
    return absolute ? "/" : ".";
  }
  if (absolute && end === 1) {
    return "//";
  }
  return path.slice(0, end);
}

function basename(path, suffix) {
  assertPath(path);
  let end = path.length;
  while (end > 0 && path.charCodeAt(end - 1) === 47) {
    end -= 1;
  }
  let start = 0;
  for (let at = end - 1; at >= 0; at -= 1) {
    if (path.charCodeAt(at) === 47) {
      start = at + 1;
      break;
    }
  }
  const name = path.slice(start, end);
  if (
    typeof suffix === "string" &&
    suffix.length > 0 &&
    suffix.length < name.length &&
    name.endsWith(suffix)
  ) {
    return name.slice(0, name.length - suffix.length);
  }
  return name;
}

function extname(path) {
  assertPath(path);
  const name = basename(path);
  // A leading dot is the whole name and not an extension, which is
  // what makes `.bashrc` have none.
  const dot = name.lastIndexOf(".");
  return dot <= 0 ? "" : name.slice(dot);
}

function format(parsed) {
  const dir = parsed.dir ?? parsed.root ?? "";
  const base = parsed.base ?? (parsed.name ?? "") + (parsed.ext ?? "");
  if (dir.length === 0) {
    return base;
  }
  return dir === parsed.root ? dir + base : dir + "/" + base;
}

function parse(path) {
  assertPath(path);
  const root = isAbsolute(path) ? "/" : "";
  const base = basename(path);
  const ext = extname(path);
  return {
    root,
    dir: dirname(path) === "." && !path.includes("/") ? "" : dirname(path),
    base,
    ext,
    name: base.slice(0, base.length - ext.length),
  };
}

function toNamespacedPath(path) {
  return path;
}

function matchesGlob() {
  throw new TypeError("node:path matchesGlob is not implemented here");
}

const posix = {
  sep,
  delimiter,
  normalize,
  isAbsolute,
  join,
  resolve,
  relative,
  dirname,
  basename,
  extname,
  format,
  parse,
  toNamespacedPath,
  matchesGlob,
};
// The same object under both names, and each of them carries both, so
// `path.posix.win32.posix` is still a namespace however a package
// reaches through it.
const win32 = posix;
posix.posix = posix;
posix.win32 = win32;

export default posix;
export {
  sep,
  delimiter,
  normalize,
  isAbsolute,
  join,
  resolve,
  relative,
  dirname,
  basename,
  extname,
  format,
  parse,
  toNamespacedPath,
  matchesGlob,
  posix,
  win32,
};
