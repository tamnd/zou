#!/bin/sh
# One machine's row in the hardware book (docs/hardware.md).
#
# Usage: scripts/zou-hardware.sh [path] [store-target]
#
# The path is where a store would live and decides which disk and
# filesystem get reported, default the current directory. The store
# target, when given, is probed with `zou probe`, which is the distance
# half of the row: a small object round tripped for latency and a large
# one moved for bandwidth, through the same client the engine uses.
#
# Why this exists at all: a tps is a number about a pair, a box and a
# store, and the pair is what result tables leave out. A run on eight
# shared cores against a MinIO on the same disk as the WAL and a run on
# thirty two quiet ones against a bucket are the same column and not
# the same measurement. So every result is stamped with the row, and
# the row is dated because a box is not the same box six months later.
#
# Everything here is read only apart from the probe, which writes a
# handful of objects under probe/ and deletes them.
set -eu

WHERE=${1:-.}
TARGET=${2:-}
ZOU=${ZOU_BIN:-target/release}/zou

say() { printf '%s\n' "$*"; }

# Best effort throughout. A box missing lscpu or a kernel without a
# rotational flag still has a row worth writing, and a field nobody
# could measure says so rather than being left out silently.
first() {
    for candidate in "$@"; do
        if [ -n "$candidate" ]; then
            printf '%s' "$candidate"
            return
        fi
    done
    printf 'unknown'
}

trim() { sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'; }

say "date: $(date -u '+%Y-%m-%d %H:%M UTC')"
say "host: $(first "$(hostname 2>/dev/null || true)")"

case $(uname -s) in
Linux)
    KERNEL="$(uname -sr)"
    DISTRO=$(. /etc/os-release 2>/dev/null && printf '%s' "${PRETTY_NAME:-}")
    say "os: $(first "$DISTRO") on $KERNEL"

    MODEL=$(awk -F: '/^model name/ {print $2; exit}' /proc/cpuinfo 2>/dev/null | trim)
    SOCKETS=$(lscpu 2>/dev/null | awk -F: '/^Socket\(s\)/ {print $2}' | trim)
    CORES=$(lscpu 2>/dev/null | awk -F: '/^Core\(s\) per socket/ {print $2}' | trim)
    THREADS=$(nproc 2>/dev/null || true)
    say "cpu: $(first "$MODEL"), $(first "$CORES" "?") cores x $(first "$SOCKETS" "1") sockets, $(first "$THREADS") threads"

    RAM=$(awk '/^MemTotal/ {printf "%.0f GB", $2/1048576}' /proc/meminfo 2>/dev/null)
    say "memory: $(first "$RAM")"

    # The device behind the path rather than the first disk in the box,
    # since a store on a spare spindle and a store on the root nvme are
    # two different machines as far as a result is concerned.
    FS=$(df -PT "$WHERE" 2>/dev/null | awk 'NR==2 {print $2}')
    DEV=$(df -P "$WHERE" 2>/dev/null | awk 'NR==2 {print $1}')
    SIZE=$(df -Ph "$WHERE" 2>/dev/null | awk 'NR==2 {print $2}')
    FREE=$(df -Ph "$WHERE" 2>/dev/null | awk 'NR==2 {print $4}')
    OPTS=$(findmnt -no OPTIONS --target "$WHERE" 2>/dev/null | cut -d, -f1-4)
    DISK=$(lsblk -no MODEL "$DEV" 2>/dev/null | head -1 | trim)
    # A virtual disk has no model to report and answers 0 for
    # rotational whatever is underneath it, which is worth saying
    # plainly rather than printing as a fact about the hardware.
    case $(lsblk -no ROTA "$DEV" 2>/dev/null | head -1 | trim) in
    1) SPIN="rotational" ;;
    0) SPIN="non rotational as the kernel sees it" ;;
    *) SPIN="rotational unknown" ;;
    esac
    say "disk: $(first "$DEV") $(first "$FS"), $(first "$SIZE") with $(first "$FREE") free"
    say "disk model: $(first "$DISK" "none reported, which is what a virtual disk says"), $SPIN"
    say "mount: $(first "$OPTS")"
    say "load: $(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null || true)"
    ;;
Darwin)
    say "os: macOS $(sw_vers -productVersion 2>/dev/null || true) on $(uname -sr)"
    MODEL=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)
    THREADS=$(sysctl -n hw.logicalcpu 2>/dev/null || true)
    CORES=$(sysctl -n hw.physicalcpu 2>/dev/null || true)
    say "cpu: $(first "$MODEL"), $(first "$CORES") cores, $(first "$THREADS") threads"
    RAM=$(sysctl -n hw.memsize 2>/dev/null | awk '{printf "%.0f GB", $1/1073741824}')
    say "memory: $(first "$RAM")"
    FS=$(df -PT "$WHERE" 2>/dev/null | awk 'NR==2 {print $2}')
    say "disk: $(df -Ph "$WHERE" 2>/dev/null | awk 'NR==2 {print $1, $2 " with " $4 " free"}'), $(first "$FS" apfs)"
    ;;
*)
    say "os: $(uname -sr)"
    say "cpu: $(first "$(nproc 2>/dev/null || true)") threads"
    ;;
esac

if [ -n "$TARGET" ]; then
    if [ -x "$ZOU" ]; then
        "$ZOU" probe "$TARGET"
    else
        say "target: $TARGET"
        say "probe: no zou binary at $ZOU, build one or set ZOU_BIN"
    fi
fi
