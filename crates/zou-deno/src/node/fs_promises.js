// node:fs/promises, the read only half of node:fs with promises on it.
//
// Written here rather than wrapped around the callback module, so the
// two do not import each other in a circle for no reason.

import { Buffer } from "node:buffer";

const decoder = new TextDecoder();

function missing(path, why) {
  const wrong = new Error(`ENOENT: no such file or directory, open '${path}'`);
  wrong.code = "ENOENT";
  wrong.errno = -2;
  wrong.syscall = "open";
  wrong.path = path;
  wrong.cause = why;
  return wrong;
}

function shaped(bytes, options) {
  const encoding = typeof options === "string" ? options : (options?.encoding ?? null);
  if (encoding === null) {
    return Buffer.from(bytes);
  }
  return encoding === "utf8" || encoding === "utf-8"
    ? decoder.decode(bytes)
    : Buffer.from(bytes).toString(encoding);
}

export async function readFile(path, options) {
  try {
    return shaped(await Deno.readFile(String(path)), options);
  } catch (why) {
    throw missing(path, why);
  }
}

export async function stat(path) {
  const bytes = await Deno.readFile(String(path)).catch((why) => {
    throw missing(path, why);
  });
  return {
    size: bytes.length,
    isFile: () => true,
    isDirectory: () => false,
    isSymbolicLink: () => false,
    mode: 0o444,
  };
}

export const lstat = stat;

export async function access(path) {
  await readFile(path);
}

/// The path back. Nothing here follows a link, because there is no
/// listing and no link to follow: what a function has is the files
/// beside it, under the names it was given.
export async function realpath(path) {
  return String(path);
}

function readOnly(name) {
  return async function () {
    throw new TypeError(
      `the filesystem a function runs on is read only, so node:fs/promises ${name} cannot work here`,
    );
  };
}

/// The other half of what is missing, which is not about writing. A
/// function can read a file that was deployed beside it and that is the
/// whole of its filesystem, so a call that wants to walk one has
/// nothing to walk rather than something it is not allowed to touch.
function nothingToRead(name) {
  return async function () {
    throw new TypeError(
      `a function reads the files deployed beside it, so node:fs/promises ${name} has nothing to work on here`,
    );
  };
}

export const writeFile = readOnly("writeFile");
export const appendFile = readOnly("appendFile");
export const mkdir = readOnly("mkdir");
export const rm = readOnly("rm");
export const rmdir = readOnly("rmdir");
export const unlink = readOnly("unlink");
export const rename = readOnly("rename");
export const open = readOnly("open");
export const mkdtemp = readOnly("mkdtemp");
export const chmod = readOnly("chmod");
export const lchmod = readOnly("lchmod");
export const chown = readOnly("chown");
export const lchown = readOnly("lchown");
export const utimes = readOnly("utimes");
export const lutimes = readOnly("lutimes");
export const link = readOnly("link");
export const symlink = readOnly("symlink");
export const truncate = readOnly("truncate");
export const copyFile = readOnly("copyFile");
export const cp = readOnly("cp");

export const readdir = nothingToRead("readdir");
export const opendir = nothingToRead("opendir");
export const readlink = nothingToRead("readlink");
export const statfs = nothingToRead("statfs");
export const watch = nothingToRead("watch");
export const glob = nothingToRead("glob");

/// The numbers node's flags are, which a package builds an argument out
/// of before it calls anything. They are constants rather than
/// behaviour, so they are the same numbers here.
export const constants = Object.freeze({
  F_OK: 0,
  X_OK: 1,
  W_OK: 2,
  R_OK: 4,
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
});

export default {
  access,
  appendFile,
  chmod,
  chown,
  constants,
  copyFile,
  cp,
  glob,
  lchmod,
  lchown,
  link,
  lstat,
  lutimes,
  mkdir,
  mkdtemp,
  open,
  opendir,
  readFile,
  readdir,
  readlink,
  realpath,
  rename,
  rm,
  rmdir,
  stat,
  statfs,
  symlink,
  truncate,
  unlink,
  utimes,
  watch,
  writeFile,
};
