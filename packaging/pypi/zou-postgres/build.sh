#!/usr/bin/env bash
# Turn a release bundle into the zou-postgres wheel for this platform.
#
#   scripts/zou-bundle.sh
#   packaging/pypi/zou-postgres/build.sh
#   packaging/pypi/zou-postgres/build.sh --version 0.2.0 --out /tmp/wheels
#
# The wheel is a directory of binaries, so it is not a pure python
# wheel and it must not be tagged as one: pip would happily hand a mac
# postgres to a linux. There is no wheel tag for "the postgres that was
# built here", so the tag says what the binaries actually need, which on
# linux is the glibc of the machine that built them.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
bundle=""
version=""
out="$root/dist/wheels"

while [ $# -gt 0 ]; do
  case "$1" in
    --bundle) bundle="$2"; shift 2 ;;
    --version) version="$2"; shift 2 ;;
    --out) out="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

case "$(uname -s)" in
  Darwin) os=darwin ;;
  Linux) os=linux ;;
  *) echo "$(uname -s) is not a platform this ships on yet" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  arm64|aarch64) arch=arm64; machine=aarch64 ;;
  x86_64|amd64) arch=x64; machine=x86_64 ;;
  *) echo "$(uname -m) is not a platform this ships on yet" >&2; exit 1 ;;
esac

[ -n "$bundle" ] || bundle="$root/dist/zou-$os-$arch"
if [ ! -x "$bundle/pg/bin/postgres" ]; then
  echo "no bundle at $bundle, run scripts/zou-bundle.sh first" >&2
  exit 1
fi

if [ "$os" = linux ]; then
  # The bundle takes openssl and icu from the distribution, so the
  # oldest glibc it runs against is the one it was built against. Saying
  # manylinux_2_28 on a wheel built on a 2.39 machine would be a lie pip
  # has no way to check.
  glibc="$(ldd --version | head -1 | awk '{print $NF}')"
  tag="manylinux_${glibc%%.*}_${glibc#*.}_$machine"
else
  # The mac in the bundle carries homebrew's openssl and lz4 and zstd,
  # and a homebrew bottle is built for the macos it was poured on, so
  # the floor is the machine that assembled this rather than some older
  # version the compiler would have been happy to target.
  case "$arch" in
    arm64) machine=arm64 ;;
    x64) machine=x86_64 ;;
  esac
  tag="macosx_$(sw_vers -productVersion | cut -d. -f1)_0_$machine"
fi

rm -rf "$here/zou_postgres/pg" "$here/build" "$here/dist"
cp -R "$bundle/pg" "$here/zou_postgres/pg"

python="${PYTHON:-python3}"
"$python" -m pip install --quiet --upgrade build wheel setuptools
if [ -n "$version" ]; then
  # A tag builds the wheel with the tag's version, without a checkout
  # that has been edited: the file on disk stays at whatever it says.
  ZOU_POSTGRES_VERSION="$version" "$python" - "$here" <<'PY'
import pathlib, sys, os, re
f = pathlib.Path(sys.argv[1]) / "pyproject.toml"
f.write_text(re.sub(r'^version = ".*"$', 'version = "%s"' % os.environ["ZOU_POSTGRES_VERSION"], f.read_text(), count=1, flags=re.M))
PY
fi

(cd "$here" && "$python" -m build --wheel --no-isolation)
(cd "$here" && "$python" -m wheel tags --platform-tag "$tag" --python-tag py3 --abi-tag none --remove dist/*.whl)

mkdir -p "$out"
cp "$here"/dist/*.whl "$out/"
rm -rf "$here/zou_postgres/pg" "$here/build" "$here/dist"

echo
for wheel in "$out"/zou_postgres-*.whl; do
  printf '  %-52s %5.1f MB\n' "$(basename "$wheel")" \
    "$(du -k "$wheel" | awk '{print $1 / 1024}')"
done
