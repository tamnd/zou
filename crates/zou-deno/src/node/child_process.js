// node:child_process. A function does not get a process to start and
// is never going to: it is one isolate among other people's on a
// server, and the whole point of that arrangement is that nothing in
// it can reach the machine.
//
// So why is this here at all. Because a module that is refused by name
// is a module the package cannot import, and a package that imports
// this at the top and calls it in a branch nobody takes runs perfectly
// well without ever reaching it. That is not a rare shape, it is what
// the registry's browser build of these packages already does and the
// difference between the two builds on the examples corpus: an sdk
// that can shell out to ffmpeg if you ask it to, imported by a
// function that never asks. See #593.
//
// Every function here therefore exists, has the right name, and
// throws when it is called, saying the thing that is actually true.

function noProcesses(name) {
  return function () {
    throw new TypeError(
      `a function has no processes to start, so node:child_process ${name} cannot work here`,
    );
  };
}

export const spawn = noProcesses("spawn");
export const spawnSync = noProcesses("spawnSync");
export const exec = noProcesses("exec");
export const execSync = noProcesses("execSync");
export const execFile = noProcesses("execFile");
export const execFileSync = noProcesses("execFileSync");
export const fork = noProcesses("fork");

/// The class the callers that do not call `spawn` reach for, usually
/// to check whether something they were handed is one. Constructing it
/// is starting a process, so that is where the refusal is, and an
/// `instanceof` against it answers false the way it would on node for
/// anything that is not one.
export class ChildProcess {
  constructor() {
    throw new TypeError("a function has no processes to start, so a ChildProcess cannot be made here");
  }
}

export default {
  spawn,
  spawnSync,
  exec,
  execSync,
  execFile,
  execFileSync,
  fork,
  ChildProcess,
};
