#!/usr/bin/env bash
# Build the shared library, build the C program against the header, and
# run it. This is the whole C ABI story: a C compiler, one header, one
# -lzou, and nothing generated.
#
#   ZOU_PG_BIN=$PWD/build/pg/bin crates/libzou/tests/smoke.sh
#
# ZOU_PG_BIN has to name a patched postgres install, since the program
# opens a real project. CARGO_PROFILE picks debug or release, and
# CARGO_TARGET_DIR is respected if the caller sets one.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
profile="${CARGO_PROFILE:-debug}"
target="${CARGO_TARGET_DIR:-$root/target}"
libs="$target/$profile"

if [ -z "${ZOU_PG_BIN:-}" ]; then
  echo "ZOU_PG_BIN is not set, see docs/postgres.md for building one" >&2
  exit 1
fi

build=(cargo build -p libzou)
if [ "$profile" = "release" ]; then
  build+=(--release)
fi
"${build[@]}"

cc=${CC:-cc}
out="$libs/zou-smoke"
"$cc" -std=c11 -Wall -Wextra -Werror \
  -I "$here/../include" \
  -o "$out" "$here/smoke.c" \
  -L "$libs" -lzou -Wl,-rpath,"$libs"

# Rust names a cdylib after itself rather than after where it is, so
# some linkers still want to be told at run time as well.
export LD_LIBRARY_PATH="$libs:${LD_LIBRARY_PATH:-}"
export DYLD_LIBRARY_PATH="$libs:${DYLD_LIBRARY_PATH:-}"
exec "$out"
