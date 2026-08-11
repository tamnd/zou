#!/usr/bin/env node
// Hand the arguments to the binary the postinstall fetched.
//
// This is a shim rather than a wrapper: signals, exit codes and the
// terminal all belong to the child, because `zou dev` is a process a
// person leaves running and interrupts with ctrl-c.

const path = require("node:path");
const fs = require("node:fs");
const { spawnSync } = require("node:child_process");

const name = `zou-${{ darwin: "darwin", linux: "linux" }[process.platform]}-${
  { x64: "x64", arm64: "arm64" }[process.arch]
}`;
const binary = path.join(__dirname, "..", "vendor", name, "bin", "zou");

if (!fs.existsSync(binary)) {
  console.error("zou: the bundle is not here, run: npm rebuild zou-cli");
  process.exit(1);
}

const done = spawnSync(binary, process.argv.slice(2), { stdio: "inherit" });
if (done.error) {
  console.error(`zou: ${done.error.message}`);
  process.exit(1);
}
// A child that died of a signal is reported as one, so a ctrl-c looks
// like a ctrl-c to whatever ran this.
if (done.signal) {
  process.kill(process.pid, done.signal);
}
process.exit(done.status ?? 1);
