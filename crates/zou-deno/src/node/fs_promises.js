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

function readOnly(name) {
  return async function () {
    throw new TypeError(
      `the filesystem a function runs on is read only, so node:fs/promises ${name} cannot work here`,
    );
  };
}

export const writeFile = readOnly("writeFile");
export const appendFile = readOnly("appendFile");
export const mkdir = readOnly("mkdir");
export const rm = readOnly("rm");
export const unlink = readOnly("unlink");
export const rename = readOnly("rename");
export const readdir = readOnly("readdir");
export const open = readOnly("open");
export const mkdtemp = readOnly("mkdtemp");

export default {
  readFile,
  stat,
  lstat,
  access,
  writeFile,
  appendFile,
  mkdir,
  rm,
  unlink,
  rename,
  readdir,
  open,
  mkdtemp,
};
