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

/// The path back, the same answer `node:fs/promises` gives: nothing
/// here follows a link, because a function's filesystem is the files
/// deployed beside it under the names it was given.
export function realpathSync(path) {
  return String(path);
}

export function realpath(path, options, back) {
  const answer = typeof options === "function" ? options : back;
  answer(null, String(path));
}

realpath.native = realpath;
realpathSync.native = realpathSync;

function readOnly(name) {
  return function () {
    throw new TypeError(`the filesystem a function runs on is read only, so node:fs ${name} cannot work here`);
  };
}

/// The other half of what is missing, which is not about writing. A
/// function reads the files deployed beside it and that is the whole of
/// its filesystem, so a call that wants to walk one has nothing to walk
/// rather than something it is not allowed to touch.
function nothingToRead(name) {
  return function () {
    throw new TypeError(`a function reads the files deployed beside it, so node:fs ${name} has nothing to work on here`);
  };
}

export const writeFile = readOnly("writeFile");
export const writeFileSync = readOnly("writeFileSync");
export const appendFile = readOnly("appendFile");
export const appendFileSync = readOnly("appendFileSync");
export const mkdir = readOnly("mkdir");
export const mkdirSync = readOnly("mkdirSync");
export const mkdtemp = readOnly("mkdtemp");
export const mkdtempSync = readOnly("mkdtempSync");
export const rm = readOnly("rm");
export const rmSync = readOnly("rmSync");
export const rmdir = readOnly("rmdir");
export const rmdirSync = readOnly("rmdirSync");
export const unlink = readOnly("unlink");
export const unlinkSync = readOnly("unlinkSync");
export const rename = readOnly("rename");
export const renameSync = readOnly("renameSync");
export const copyFile = readOnly("copyFile");
export const copyFileSync = readOnly("copyFileSync");
export const cp = readOnly("cp");
export const cpSync = readOnly("cpSync");
export const chmod = readOnly("chmod");
export const chmodSync = readOnly("chmodSync");
export const chown = readOnly("chown");
export const chownSync = readOnly("chownSync");
export const lchmod = readOnly("lchmod");
export const lchmodSync = readOnly("lchmodSync");
export const lchown = readOnly("lchown");
export const lchownSync = readOnly("lchownSync");
export const utimes = readOnly("utimes");
export const utimesSync = readOnly("utimesSync");
export const lutimes = readOnly("lutimes");
export const lutimesSync = readOnly("lutimesSync");
export const link = readOnly("link");
export const linkSync = readOnly("linkSync");
export const symlink = readOnly("symlink");
export const symlinkSync = readOnly("symlinkSync");
export const truncate = readOnly("truncate");
export const truncateSync = readOnly("truncateSync");
export const ftruncate = readOnly("ftruncate");
export const ftruncateSync = readOnly("ftruncateSync");
export const createWriteStream = readOnly("createWriteStream");
export const openSync = readOnly("openSync");
export const open = readOnly("open");
export const write = readOnly("write");
export const writeSync = readOnly("writeSync");

export const readdir = nothingToRead("readdir");
export const readdirSync = nothingToRead("readdirSync");
export const opendir = nothingToRead("opendir");
export const opendirSync = nothingToRead("opendirSync");
export const readlink = nothingToRead("readlink");
export const readlinkSync = nothingToRead("readlinkSync");
export const statfs = nothingToRead("statfs");
export const statfsSync = nothingToRead("statfsSync");
export const watch = nothingToRead("watch");
export const watchFile = nothingToRead("watchFile");
export const glob = nothingToRead("glob");
export const globSync = nothingToRead("globSync");

/// A descriptor is what `open` would have handed back, and nothing here
/// hands one out, so the calls that take one have none to be given.
export const close = nothingToRead("close");
export const closeSync = nothingToRead("closeSync");
export const read = nothingToRead("read");
export const readSync = nothingToRead("readSync");
export const fstat = nothingToRead("fstat");
export const fstatSync = nothingToRead("fstatSync");

/// A watcher nobody set up has nothing to take down, which is a call
/// that does nothing rather than one that refuses.
export function unwatchFile() {}

export const constants = {
  F_OK: 0,
  R_OK: 4,
  W_OK: 2,
  X_OK: 1,
  COPYFILE_EXCL: 1,
  COPYFILE_FICLONE: 2,
  COPYFILE_FICLONE_FORCE: 4,
  O_RDONLY: 0,
  O_WRONLY: 1,
  O_RDWR: 2,
  O_CREAT: 64,
  O_EXCL: 128,
  O_TRUNC: 512,
  O_APPEND: 1024,
};

export { promises };

export default {
  access,
  accessSync,
  appendFile,
  appendFileSync,
  chmod,
  chmodSync,
  chown,
  chownSync,
  close,
  closeSync,
  constants,
  copyFile,
  copyFileSync,
  cp,
  cpSync,
  createReadStream,
  createWriteStream,
  exists,
  existsSync,
  fstat,
  fstatSync,
  ftruncate,
  ftruncateSync,
  glob,
  globSync,
  lchmod,
  lchmodSync,
  lchown,
  lchownSync,
  link,
  linkSync,
  lstat,
  lstatSync,
  lutimes,
  lutimesSync,
  mkdir,
  mkdirSync,
  mkdtemp,
  mkdtempSync,
  open,
  openSync,
  opendir,
  opendirSync,
  promises,
  read,
  readFile,
  readFileSync,
  readSync,
  readdir,
  readdirSync,
  readlink,
  readlinkSync,
  realpath,
  realpathSync,
  rename,
  renameSync,
  rm,
  rmSync,
  rmdir,
  rmdirSync,
  stat,
  statSync,
  statfs,
  statfsSync,
  symlink,
  symlinkSync,
  truncate,
  truncateSync,
  unlink,
  unlinkSync,
  unwatchFile,
  utimes,
  utimesSync,
  watch,
  watchFile,
  write,
  writeFile,
  writeFileSync,
  writeSync,
};
