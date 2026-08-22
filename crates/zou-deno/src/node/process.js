// node:process, which is the global one. Two objects would mean a
// package that set `process.env.X` through the module and read it off
// the global saw nothing, so there is one and this module points at it.

const process = globalThis.process;

export default process;
export const env = process.env;
export const argv = process.argv;
export const platform = process.platform;
export const arch = process.arch;
export const version = process.version;
export const versions = process.versions;
export const pid = process.pid;
export const stdout = process.stdout;
export const stderr = process.stderr;
export const stdin = process.stdin;
export const nextTick = process.nextTick;
export const cwd = process.cwd;
export const exit = process.exit;
export const hrtime = process.hrtime;
export const uptime = process.uptime;
export const memoryUsage = process.memoryUsage;
export const emitWarning = process.emitWarning;
