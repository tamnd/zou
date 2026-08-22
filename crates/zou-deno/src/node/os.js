// node:os. A function does not have a machine in the sense this module
// means: it has an isolate on a server that is running other people's
// functions too, and how much memory that server has is not something
// one tenant is told.
//
// So the answers are the ones that are true and safe to say. A package
// asking for `EOL` or `platform` gets a real answer, and a package
// asking how many cpus there are to size a worker pool gets one, which
// is the number of workers it may usefully start here.

export const EOL = "\n";

export function platform() {
  return "linux";
}

export function arch() {
  return "x86_64";
}

export function type() {
  return "Linux";
}

export function release() {
  return "0.0.0";
}

export function version() {
  return "zou";
}

export function hostname() {
  return "localhost";
}

export function homedir() {
  return "/";
}

export function tmpdir() {
  return "/tmp";
}

export function endianness() {
  return "LE";
}

export function cpus() {
  return [];
}

export function totalmem() {
  return 0;
}

export function freemem() {
  return 0;
}

export function uptime() {
  return 0;
}

export function loadavg() {
  return [0, 0, 0];
}

export function networkInterfaces() {
  return {};
}

export function userInfo() {
  return { uid: 0, gid: 0, username: "zou", homedir: "/", shell: null };
}

export const constants = { signals: {}, errno: {} };
export const devNull = "/dev/null";

export default {
  EOL,
  platform,
  arch,
  type,
  release,
  version,
  hostname,
  homedir,
  tmpdir,
  endianness,
  cpus,
  totalmem,
  freemem,
  uptime,
  loadavg,
  networkInterfaces,
  userInfo,
  constants,
  devNull,
};
