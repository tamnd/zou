// Fetch the bundle for this platform after npm has installed the
// package.
//
// The tarball is the same one install.sh downloads and the same one a
// release publishes: the zou binary and the patched postgres it starts.
// npm cannot carry it as package content, because a package with every
// platform in it is four times the size for anyone and there is no way
// to ship a fifty megabyte postgres per architecture inside one tarball
// that people would forgive.
//
//   npm i -g zou-cli
//   ZOU_VERSION=v0.1.0 npm i -g zou-cli
//
// ZOU_SKIP_DOWNLOAD=1 leaves the package unusable on purpose, for a CI
// image that will mount a bundle in later.

const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
const { execFileSync } = require("node:child_process");

const REPO = process.env.ZOU_REPO || "tamnd/zou";
const HERE = __dirname;
const VENDOR = path.join(HERE, "vendor");

function platform() {
  const os = { darwin: "darwin", linux: "linux" }[process.platform];
  const arch = { x64: "x64", arm64: "arm64" }[process.arch];
  if (!os || !arch) {
    throw new Error(`zou has no build for ${process.platform} ${process.arch} yet`);
  }
  return `zou-${os}-${arch}`;
}

async function get(url) {
  const answer = await fetch(url, { redirect: "follow" });
  if (!answer.ok) {
    throw new Error(`${url} said ${answer.status}`);
  }
  return answer;
}

// The tag that matches this package. A release stamps the version in
// package.json from the tag it is building, so `npm i zou-cli@0.2.0`
// installs the binary from v0.2.0 rather than whatever is newest, which
// is the one thing a version number is for.
function version() {
  return process.env.ZOU_VERSION || `v${require("./package.json").version}`;
}

async function main() {
  if (process.env.ZOU_SKIP_DOWNLOAD) {
    console.log("zou: skipping the download, nothing will run until a bundle is there");
    return;
  }
  const name = platform();
  const tag = version();
  const base = process.env.ZOU_BASE_URL || `https://github.com/${REPO}/releases/download/${tag}`;
  console.log(`zou ${tag} for ${name.slice(4)}`);

  const tarball = await (await get(`${base}/${name}.tar.gz`)).arrayBuffer();
  const published = (await (await get(`${base}/${name}.tar.gz.sha256`)).text()).split(/\s+/)[0];
  const got = crypto.createHash("sha256").update(Buffer.from(tarball)).digest("hex");
  if (got !== published) {
    throw new Error("the download does not match its checksum, refusing it");
  }

  fs.rmSync(VENDOR, { recursive: true, force: true });
  fs.mkdirSync(VENDOR, { recursive: true });
  const archive = path.join(VENDOR, `${name}.tar.gz`);
  fs.writeFileSync(archive, Buffer.from(tarball));
  // tar is on every platform this package installs on, and shelling out
  // to it is smaller than a dependency that unpacks tarballs.
  execFileSync("tar", ["-xzf", archive, "-C", VENDOR], { stdio: "inherit" });
  fs.rmSync(archive);
  console.log(`zou is at ${path.join(VENDOR, name, "bin", "zou")}`);
}

main().catch((e) => {
  console.error(`zou: ${e.message}`);
  process.exit(1);
});
