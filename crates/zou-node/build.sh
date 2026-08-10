#!/usr/bin/env bash
# Build the addon and put it where index.js looks for it.
#
# napi-rs has a cli that does this and a lot else. This does the part
# that matters, which is that node loads a cdylib under a name ending in
# .node, so the whole build is cargo plus a copy.
#
#   crates/zou-node/build.sh            # debug
#   CARGO_PROFILE=release crates/zou-node/build.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"
profile="${CARGO_PROFILE:-debug}"
target="${CARGO_TARGET_DIR:-$root/target}"

build=(cargo build -p zou-node)
if [ "$profile" = "release" ]; then
  build+=(--release)
fi
"${build[@]}"

built="$target/$profile/libzou_node.dylib"
if [ ! -f "$built" ]; then
  built="$target/$profile/libzou_node.so"
fi
if [ ! -f "$built" ]; then
  echo "no addon at $target/$profile/libzou_node.{dylib,so}" >&2
  exit 1
fi

cp "$built" "$here/zou.node"
echo "$here/zou.node"
