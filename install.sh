#!/bin/sh
# Install zou.
#
#   curl -fsSL https://raw.githubusercontent.com/tamnd/zou/main/install.sh | sh
#
# Downloads the release bundle for this machine, checks it against the
# sha256 published next to it, and unpacks it under ~/.zou. A bundle is
# the zou binary and the patched postgres it starts, because one without
# the other cannot open a database.
#
#   ZOU_VERSION=v0.1.0 sh install.sh    a version other than the latest
#   ZOU_HOME=/opt/zou sh install.sh     somewhere other than ~/.zou
#
# Nothing is installed system wide and nothing is run as root: the
# install is a directory, and uninstalling is removing it.
set -eu

repo=${ZOU_REPO:-tamnd/zou}
home=${ZOU_HOME:-$HOME/.zou}

case "$(uname -s)" in
  Darwin) os=darwin ;;
  Linux) os=linux ;;
  *) echo "zou has no build for $(uname -s) yet" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  arm64|aarch64) arch=arm64 ;;
  x86_64|amd64) arch=x64 ;;
  *) echo "zou has no build for $(uname -m) yet" >&2; exit 1 ;;
esac

fetch() {
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$1" -o "$2"
  elif command -v wget >/dev/null 2>&1; then
    wget -q "$1" -O "$2"
  else
    echo "this needs curl or wget" >&2
    exit 1
  fi
}

version=${ZOU_VERSION:-}
if [ -z "$version" ]; then
  # The tag of the latest release, out of the json, without asking for a
  # json parser to be installed.
  latest=$(mktemp)
  fetch "https://api.github.com/repos/$repo/releases/latest" "$latest"
  version=$(sed -n 's/.*"tag_name" *: *"\([^"]*\)".*/\1/p' "$latest" | head -1)
  rm -f "$latest"
  if [ -z "$version" ]; then
    echo "could not work out the latest version of $repo, set ZOU_VERSION" >&2
    exit 1
  fi
fi

name="zou-$os-$arch"
url="https://github.com/$repo/releases/download/$version/$name.tar.gz"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

echo "zou $version for $os $arch"
fetch "$url" "$work/$name.tar.gz"
fetch "$url.sha256" "$work/$name.tar.gz.sha256"

want=$(awk '{print $1}' "$work/$name.tar.gz.sha256")
if command -v sha256sum >/dev/null 2>&1; then
  got=$(sha256sum "$work/$name.tar.gz" | awk '{print $1}')
elif command -v shasum >/dev/null 2>&1; then
  got=$(shasum -a 256 "$work/$name.tar.gz" | awk '{print $1}')
else
  echo "this needs sha256sum or shasum" >&2
  exit 1
fi
if [ "$want" != "$got" ]; then
  echo "the download does not match its checksum, refusing it" >&2
  exit 1
fi

tar -xzf "$work/$name.tar.gz" -C "$work"

# Versions live side by side and the link says which one is current, so
# a second install is a link change rather than a directory that is half
# one version and half another.
mkdir -p "$home/versions" "$home/bin"
rm -rf "$home/versions/$version"
mv "$work/$name" "$home/versions/$version"
ln -sfn "$home/versions/$version/bin/zou" "$home/bin/zou"

echo "installed to $home/versions/$version"
"$home/bin/zou" --version

case ":$PATH:" in
  *":$home/bin:"*) ;;
  *)
    echo
    echo "add it to your path:"
    echo "  export PATH=\"$home/bin:\$PATH\""
    ;;
esac
echo
echo "then: zou dev ./data"
