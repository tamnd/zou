#!/usr/bin/env bash
# Build the extension and put it where the package looks for it.
#
# maturin does this and a lot else, and building the wheels is its job.
# This does the part that matters for running the thing out of the tree,
# which is that python imports a cdylib named after the module, so the
# whole build is cargo plus a copy.
#
#   crates/zou-python/build.sh            # debug
#   CARGO_PROFILE=release crates/zou-python/build.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"
profile="${CARGO_PROFILE:-debug}"
target="${CARGO_TARGET_DIR:-$root/target}"

build=(cargo build -p zou-python)
if [ "$profile" = "release" ]; then
  build+=(--release)
fi
"${build[@]}"

built="$target/$profile/libzou_python.dylib"
if [ ! -f "$built" ]; then
  built="$target/$profile/libzou_python.so"
fi
if [ ! -f "$built" ]; then
  echo "no extension at $target/$profile/libzou_python.{dylib,so}" >&2
  exit 1
fi

# .abi3.so on both, because the build is abi3 and one file serves every
# python from 3.9 up. macOS loads a .so extension the same as any other.
cp "$built" "$here/python/zou/_zou.abi3.so"
echo "$here/python/zou/_zou.abi3.so"
