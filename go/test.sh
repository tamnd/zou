#!/usr/bin/env bash
# Build the library the cgo package links against, then run the tests.
#
# The package points at target/debug and bakes an rpath to it, so the
# whole build is cargo plus go test. CARGO_PROFILE=release builds the
# fast one and points the linker at it.
#
#   ZOU_PG_BIN=$PWD/build/pg/bin go/test.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/.." && pwd)"
profile="${CARGO_PROFILE:-debug}"
target="${CARGO_TARGET_DIR:-$root/target}"
libs="$target/$profile"

build=(cargo build -p libzou)
if [ "$profile" = "release" ]; then
  build+=(--release)
fi
"${build[@]}"

if [ "$profile" != "debug" ] || [ -n "${CARGO_TARGET_DIR:-}" ]; then
  # The package's own flags name target/debug. Anything else has to be
  # said here, and cgo takes these ahead of the directive.
  export CGO_LDFLAGS="-L$libs -lzou -Wl,-rpath,$libs"
fi
export LD_LIBRARY_PATH="$libs:${LD_LIBRARY_PATH:-}"
export DYLD_LIBRARY_PATH="$libs:${DYLD_LIBRARY_PATH:-}"

cd "$here"
exec go test "$@" ./...
