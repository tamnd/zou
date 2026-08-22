// node:fs. A function's filesystem is the static files it was deployed
// with, read only, and the host decides what is in it. So this module
// reads and does not write, and everything that would write is a
// sentence saying so rather than a silent success.
//
// The reads go through `Deno.readFile`, which is the same op with the
// same rule about what a function may open, so there is one place that
// decides and not two.

import { Buffer } from "node:buffer";
import { Readable } from "node:stream";
import promises from "node:fs/promises";

const decoder = new TextDecoder();

/// Node's error for a file that is not there, which is what a package
/// catches by code when it is deciding whether to fall back.
function missing(path, why) {
  const wrong = new Error(`ENOENT: no such file or directory, open '${path}'`);
  wrong.code = "ENOENT";
  wrong.errno = -2;
  wrong.syscall = "open";
  wrong.path = path;
  wrong.cause = why;
  return wrong;
}

function encodingOf(options) {
  if (typeof options === "string") {
    return options;
  }
  return options?.encoding ?? null;
}

function shaped(bytes, options) {
  const encoding = encodingOf(options);
  if (encoding === null) {
    return Buffer.from(bytes);
  }
  return encoding === "utf8" || encoding === "utf-8"
    ? decoder.decode(bytes)
    : Buffer.from(bytes).toString(encoding);
}

export function readFileSync(path, options) {
  try {
    return shaped(Deno.readFileSync(String(path)), options);
  } catch (why) {
    throw missing(path, why);
  }
}

export function readFile(path, options, back) {
  if (typeof options === "function") {
    back = options;
    options = null;
  }
  Deno.readFile(String(path)).then(
    (bytes) => back(null, shaped(bytes, options)),
    (why) => back(missing(path, why)),
  );
}

export function existsSync(path) {
  try {
    Deno.readFileSync(String(path));
    return true;
  } catch {
    return false;
  }
}

export function exists(path, back) {
  back(existsSync(path));
}

export function accessSync(path) {
  if (!existsSync(path)) {
    throw missing(path);
  }
}

export function access(path, mode, back) {
  if (typeof mode === "function") {
    back = mode;
  }
  back(existsSync(path) ? null : missing(path));
}

/// Enough of a stat that a package can ask whether something is a file
/// and how big it is. There is no inode here and no times, and saying
/// zero for a time would be worse than not having the field.
export function statSync(path) {
  const bytes = Deno.readFileSync(String(path));
  return {
    size: bytes.length,
    isFile: () => true,
    isDirectory: () => false,
    isSymbolicLink: () => false,
    mode: 0o444,
  };
}

export function stat(path, back) {
  try {
    back(null, statSync(path));
  } catch (why) {
    back(missing(path, why));
  }
}

export const lstatSync = statSync;
export const lstat = stat;

/// A file as a stream, which is how a package hands one to something
/// that takes a stream. The whole file is read first, because the read
/// this runtime has is whole file at a time.
export function createReadStream(path, options) {
  const made = new Readable();
  Deno.readFile(String(path)).then(
    (bytes) => {
      made.push(encodingOf(options) === null ? Buffer.from(bytes) : shaped(bytes, options));
      made.push(null);
    },
    (why) => made.destroy(missing(path, why)),
  );
  return made;
}

function readOnly(name) {
  return function () {
    throw new TypeError(`the filesystem a function runs on is read only, so node:fs ${name} cannot work here`);
  };
}

export const writeFile = readOnly("writeFile");
export const writeFileSync = readOnly("writeFileSync");
export const appendFile = readOnly("appendFile");
export const appendFileSync = readOnly("appendFileSync");
export const mkdir = readOnly("mkdir");
export const mkdirSync = readOnly("mkdirSync");
export const rm = readOnly("rm");
export const rmSync = readOnly("rmSync");
export const unlink = readOnly("unlink");
export const unlinkSync = readOnly("unlinkSync");
export const rename = readOnly("rename");
export const renameSync = readOnly("renameSync");
export const createWriteStream = readOnly("createWriteStream");
export const readdir = readOnly("readdir");
export const readdirSync = readOnly("readdirSync");
export const watch = readOnly("watch");
export const openSync = readOnly("openSync");
export const open = readOnly("open");

export const constants = {
  F_OK: 0,
  R_OK: 4,
  W_OK: 2,
  X_OK: 1,
};

export { promises };

export default {
  readFile,
  readFileSync,
  exists,
  existsSync,
  access,
  accessSync,
  stat,
  statSync,
  lstat,
  lstatSync,
  createReadStream,
  createWriteStream,
  writeFile,
  writeFileSync,
  appendFile,
  appendFileSync,
  mkdir,
  mkdirSync,
  rm,
  rmSync,
  unlink,
  unlinkSync,
  rename,
  renameSync,
  readdir,
  readdirSync,
  watch,
  open,
  openSync,
  constants,
  promises,
};
