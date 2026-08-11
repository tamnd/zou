#!/usr/bin/env bash
# Assemble what a user actually downloads, and say what it weighs.
#
# A zou release is not one binary: postgres is a child process, so the
# patched build ships with it, and the budget in milestone 3 is 150 MB
# for the pair. This puts the two together the way an installer would
# and prints the breakdown, so the number is measured on the machine
# rather than guessed at.
#
#   scripts/zou-bundle.sh                    # build/pg plus target/release
#   scripts/zou-bundle.sh --tar              # and a tarball next to it
#   scripts/zou-bundle.sh --prefix /opt/pg --out /tmp/dist
#
# Exits non zero when the bundle is over budget, so CI notices the day
# something doubles.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
prefix="$root/build/pg"
out="$root/dist"
budget_mb=150
tar=no

while [ $# -gt 0 ]; do
  case "$1" in
    --prefix) prefix="$2"; shift 2 ;;
    --out) out="$2"; shift 2 ;;
    --budget) budget_mb="$2"; shift 2 ;;
    --tar) tar=yes; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [ ! -x "$prefix/bin/postgres" ]; then
  echo "no patched postgres at $prefix, run make pg-build first" >&2
  exit 1
fi

case "$(uname -s)" in
  Darwin) os=darwin; dl=dylib ;;
  Linux) os=linux; dl=so ;;
  *) echo "$(uname -s) is not a platform this ships on yet" >&2; exit 1 ;;
esac

case "$(uname -m)" in
  arm64|aarch64) arch=arm64 ;;
  x86_64|amd64) arch=x64 ;;
  *) echo "$(uname -m) is not a platform this ships on yet" >&2; exit 1 ;;
esac

zou="$root/target/release/zou"
if [ ! -x "$zou" ]; then
  cargo build --release -p zou --manifest-path "$root/Cargo.toml"
fi

# Postgres finds its own share and modules relative to the postmaster,
# so the bundle has to keep the layout the install has rather than one
# of its own: a debian meson build puts the modules under
# lib/x86_64-linux-gnu/postgresql and a mac one under lib/postgresql,
# and both have to keep working after they are moved.
under() {
  case "$1" in
    "$prefix"/*) echo "${1#"$prefix"/}" ;;
    *) echo "$1 is outside $prefix, which this cannot bundle" >&2; exit 1 ;;
  esac
}
pkglibdir="$(under "$("$prefix/bin/pg_config" --pkglibdir)")"
libdir="$(under "$("$prefix/bin/pg_config" --libdir)")"
sharedir="$(under "$("$prefix/bin/pg_config" --sharedir)")"

name="zou-$os-$arch"
bundle="$out/$name"
rm -rf "$bundle"
mkdir -p "$bundle/bin" "$bundle/pg/bin" "$bundle/pg/$pkglibdir" "$bundle/pg/$(dirname "$sharedir")"

cp "$zou" "$bundle/bin/zou"

# Of the thirty odd programs in a postgres install, zou starts two:
# initdb once and the postmaster after that. psql comes along because a
# person with a database wants a prompt for it, and pg_dump and
# pg_restore because getting data out is not optional. The rest is
# tooling for a postgres that is administered by hand, which this is
# not.
for program in postgres initdb psql pg_dump pg_restore; do
  cp "$prefix/bin/$program" "$bundle/pg/bin/$program"
done

# The loadable modules, minus the three language handlers whose
# interpreters are not in the bundle: shipping plpython3 without a
# python it was linked against is a file that can only fail to load.
for module in "$prefix/$pkglibdir"/*."$dl"; do
  case "$(basename "$module")" in
    plperl.*|plpython3.*|pltcl.*|*_plperl.*|*_plpython3.*) continue ;;
  esac
  cp "$module" "$bundle/pg/$pkglibdir/"
done
# libpq is here for the modules that link it, dblink and
# postgres_fdw and the walreceiver, not for anything zou calls.
for lib in "$prefix/$libdir"/libpq*; do
  case "$lib" in *.a) continue ;; esac
  cp -P "$lib" "$bundle/pg/$libdir/"
done

# initdb reads postgres.bki, the timezone database, and the rest of it
# out of share, and create extension reads the sql there, so this goes
# whole. The documentation does not.
cp -R "$prefix/$sharedir" "$bundle/pg/$sharedir"
rm -rf "$bundle/pg/$sharedir/man"

# The rust binary is stripped by the release profile; postgres is not,
# and its symbols are a third of it.
case "$os" in
  darwin) strip -x "$bundle/pg/bin"/* "$bundle/pg/$pkglibdir"/*."$dl" 2>/dev/null || true ;;
  linux) strip --strip-unneeded "$bundle/pg/bin"/* "$bundle/pg/$pkglibdir"/*."$dl" 2>/dev/null || true ;;
esac

# Everything postgres builds is linked against the prefix it was built
# in, by absolute path: `/root/zou/build/pg/lib` is written into initdb
# and stays there when the file is copied somewhere else. A tarball that
# only runs on the machine that made it is not a release, so the paths
# get rewritten to ones relative to the file holding them, and after
# this the bundle runs wherever it is unpacked.
origin() {
  case "$os" in darwin) echo "@loader_path" ;; linux) echo '$ORIGIN' ;; esac
}

# The way back to the directory libpq is in, from a file in $1, where $1
# is relative to the pg prefix. bin is one level down, the modules under
# lib are two or three depending on the platform.
back_to_libdir() {
  if [ "$1" = "$libdir" ]; then
    origin
  else
    echo "$(origin)/$(echo "$1" | awk -F/ '{ up = ".."; for (i = 2; i <= NF; i++) up = up "/.."; print up }')/$libdir"
  fi
}

machos() {
  find "$bundle/pg/bin" -type f
  find "$bundle/pg/lib" -type f -name "*.$dl*"
}

if [ "$os" = darwin ]; then
  # A mac postgres links homebrew: openssl, lz4, zstd. Homebrew is not
  # in the bundle and is not at the same place on an intel mac as on an
  # arm one, so those come along too. Anything under /usr/lib or
  # /System is the operating system and stays where it is.
  while :; do
    before="$(machos | wc -l)"
    for file in $(machos); do
      for dep in $(otool -L "$file" | awk 'NR > 1 { print $1 }'); do
        case "$dep" in /usr/lib/*|/System/*|@*|"$prefix"/*) continue ;; esac
        [ -f "$bundle/pg/$libdir/$(basename "$dep")" ] || cp "$dep" "$bundle/pg/$libdir/"
      done
    done
    [ "$before" = "$(machos | wc -l)" ] && break
  done
fi

if [ "$os" = linux ] && ! command -v patchelf >/dev/null; then
  echo "a relocatable linux bundle needs patchelf, apt-get install patchelf" >&2
  exit 1
fi

for file in $(machos); do
  chmod u+w "$file"
  rpath="$(back_to_libdir "$(dirname "${file#"$bundle/pg/"}")")"
  case "$os" in
    darwin)
      case "$file" in
        *.dylib) install_name_tool -id "@rpath/$(basename "$file")" "$file" ;;
      esac
      for dep in $(otool -L "$file" | awk 'NR > 1 { print $1 }'); do
        case "$dep" in /usr/lib/*|/System/*|@*) continue ;; esac
        install_name_tool -change "$dep" "@rpath/$(basename "$dep")" "$file"
      done
      install_name_tool -add_rpath "$rpath" "$file" 2>/dev/null || true
      # Editing a mach-o breaks its signature, and an arm64 mac will not
      # run a file whose signature is broken.
      codesign --force --sign - "$file" 2>/dev/null || true
      ;;
    linux) patchelf --set-rpath "$rpath" "$file" ;;
  esac
done

# What is left over: the libraries the bundle expects to find on the
# machine that unpacks it. This is checked rather than trusted, because
# an optional postgres dependency is picked up from whatever the build
# machine happened to have installed, and the first person to hear about
# a new one is otherwise whoever downloaded the tarball onto a machine
# without it.
depends_on() {
  for file in $(machos); do
    case "$os" in
      linux) patchelf --print-needed "$file" ;;
      darwin) otool -L "$file" | awk 'NR > 1 { print $1 }' ;;
    esac
  done | sort -u
}

needed=""
unknown=""
for dep in $(depends_on); do
  # A relocated mach-o names its own libraries through @rpath, and
  # /usr/lib and /System are the operating system.
  case "$dep" in @*|/usr/lib/*|/System/*) continue ;; esac
  lib="$(basename "$dep")"
  [ -e "$bundle/pg/$libdir/$lib" ] && continue
  needed="$needed $lib"
  case "$lib" in
    # The c library and the things every binary on the platform links.
    ld-linux*|libc.so.*|libm.so.*|libdl.so.*|libpthread.so.*|librt.so.*|libresolv.so.*|libgcc_s.so.*|libstdc++.so.*|libcrypt.so.*) ;;
    # And the ones postgres genuinely wants: collation, a prompt,
    # compression, tls, uuids, and the xml type. The docker image
    # installs these and nothing else.
    libicu*.so.*|libreadline.so.*|libtinfo.so.*|libncurses*.so.*|libz.so.*|liblz4.so.*|libzstd.so.*|libssl.so.*|libcrypto.so.*|libuuid.so.*|libxml2.so.*|libxslt.so.*|libexslt.so.*) ;;
    *) unknown="$unknown $lib" ;;
  esac
done

if [ -n "$unknown" ]; then
  echo >&2
  echo "the bundle needs libraries a plain $os is not expected to have:$unknown" >&2
  echo "either turn that dependency off in the pg-build options, or ship the" >&2
  echo "library in the bundle and install it in the docker image" >&2
  exit 1
fi

kb() { du -sk "$1" | awk '{print $1}'; }
mb() { awk -v k="$1" 'BEGIN { printf "%.1f", k / 1024 }'; }

total_kb=$(kb "$bundle")
echo
echo "$name, from $prefix"
printf '  %-28s %8s MB\n' "zou" "$(mb "$(kb "$bundle/bin/zou")")"
printf '  %-28s %8s MB\n' "postgres" "$(mb "$(kb "$bundle/pg/bin/postgres")")"
printf '  %-28s %8s MB\n' "the other pg programs" \
  "$(mb "$(( $(kb "$bundle/pg/bin") - $(kb "$bundle/pg/bin/postgres") ))")"
printf '  %-28s %8s MB\n' "modules and the libs they use" "$(mb "$(kb "$bundle/pg/lib")")"
printf '  %-28s %8s MB\n' "share, bki and timezones" "$(mb "$(kb "$bundle/pg/share")")"
printf '  %-28s %8s MB\n' "the bundle" "$(mb "$total_kb")"

if [ "$tar" = yes ]; then
  tarball="$out/$name.tar.gz"
  tar -czf "$tarball" -C "$out" "$name"
  printf '  %-28s %8s MB\n' "the tarball" "$(mb "$(kb "$tarball")")"
fi

echo
echo "  from the machine, and nothing else:"
if [ -z "$needed" ]; then
  echo "    nothing, everything left is the operating system"
else
  for lib in $needed; do echo "    $lib"; done
fi

budget_kb=$(( budget_mb * 1024 ))
if [ "$total_kb" -gt "$budget_kb" ]; then
  echo
  echo "over the $budget_mb MB budget by $(mb "$(( total_kb - budget_kb ))") MB" >&2
  exit 1
fi
echo "  under the $budget_mb MB budget with $(mb "$(( budget_kb - total_kb ))") MB to spare"
