#!/bin/sh
# What a run costs in memory, cpu and disk, sampled while it happens.
#
# Usage: scripts/zou-usage.sh <postmaster-pid> <samples-file> [interval] [paths]
#        scripts/zou-usage.sh --report <samples-file> [from] [to] [transactions]
#        scripts/zou-usage.sh --written <samples-file> [from] [to]
#
# The first form samples until it is killed. The second reads the file
# back and prints one line for a window of it, which is how a phase
# gets its own number out of a whole run's samples.
#
# Two claims in M1b need this and neither can be answered afterwards:
# the shim's own footprint excluding page caches, and cpu seconds per
# thousand transactions against vanilla. A peak that happened during
# the load phase is not a peak the select-only phase paid for, so the
# samples are timestamped and the windows are cut afterwards rather
# than the sampler being started and stopped around each phase.
#
# Memory is Pss and not the sum of RSS. Postgres is a process per
# connection over one shared memory segment, so summing RSS across
# eight backends counts shared_buffers eight times and answers a
# multiple of the truth. Pss divides each shared page by how many
# processes map it, which adds up to what the tree actually occupies.
# It is Linux only, so elsewhere the sum of RSS is reported and said
# to be a sum of RSS.
#
# The same rollup splits that Pss into a file backed half and an
# anonymous half and a shared memory half, and that split is the
# difference between a footprint and a cache: the file backed half is
# page cache the tree is charged for and gives back under pressure, the
# anonymous half is not and does not, and the shared half is
# shared_buffers and is the same size whatever the workload does. A
# budget written against the sum of the three is written against the
# wrong number. The three add up to the Pss above them. Swap comes from
# SwapPss, divided by its mappers for the same reason Pss is, and the
# major faults come out of the process stat lines the same way the cpu
# counters do.
#
# A leak is a slope rather than a peak and it does not show up in a
# sixty second phase, so a window over an hour also gets a least squares
# fit on its memory readings, reported as MiB an hour and as a share of
# the peak. It is a rate and not a verdict: a run still filling
# shared_buffers climbs honestly and a threshold here would call that a
# leak.
#
# Cpu is the live tree's own time plus the postmaster's cutime and
# cstime, which hold the children it has already reaped. Both halves
# are needed and they do not overlap: without the child counters a
# backend that connected, worked and went away between two samples
# would not be counted at all, and without the live tree a phase's cpu
# would only appear at the disconnect that ends it.
#
# Disk is /proc/diskstats for the devices behind the paths named in the
# fourth argument, a comma separated list, usually the run directory and
# the store. It is the whole device and not this run's share of it, and
# it says so by printing the device name, because a box with somebody
# else's workload on the same disk is the normal case on our servers and
# a utilization figure that quietly included their traffic would be
# worse than no figure at all. A partition is reported as itself rather
# than as its whole disk, so two filesystems on one disk stay apart.
set -eu

ticks() {
    getconf CLK_TCK 2>/dev/null || echo 100
}

# Every pid under a root, the root included. Walked out of ppid pairs
# rather than with pgrep -P recursion, which is a fork per level per
# sample and this runs once a second for hours.
tree() {
    root=$1
    ps -eo pid=,ppid= 2>/dev/null | awk -v root="$root" '
        { parent[$1] = $2; pids[NR] = $1 }
        END {
            want[root] = 1
            # Bounded passes rather than recursion: a postgres tree is
            # two levels deep and this closes over anything sane.
            for (pass = 0; pass < 8; pass++)
                for (i = 1; i <= NR; i++)
                    if (want[parent[pids[i]]]) want[pids[i]] = 1
            for (i = 1; i <= NR; i++) if (want[pids[i]]) print pids[i]
        }'
}

# Memory and cpu in one pass over /proc, since the alternative is two
# forks per process per second and a postgres tree at 100 clients is a
# hundred processes.
#
# Cpu is the live tree's own time plus the root's cutime and cstime.
# The two halves do not overlap: a backend is either alive and in the
# tree or reaped and in the root's child counters, and it moves from
# one to the other at the moment it exits. So the total only ever goes
# up, which is what a window being a subtraction requires. The root's
# child counters alone would be a staircase instead, since pgbench
# holds its connections for a whole phase and every second of their
# cpu would land on whichever phase happened to contain the
# disconnect.
#
# A reading that could not be taken prints nothing rather than zeros.
# A backend exiting between the moment its /proc file is listed and
# the moment it is read makes awk fail on the open, and a zero row
# written for that second would show up later as a phase whose memory
# fell to nothing and whose cpu went backwards.
snapshot() {
    root=$1
    pids=$(tree "$root")
    [ -n "$pids" ] || return 0
    if [ -r /proc/self/smaps_rollup ]; then
        files=""
        for pid in $pids; do
            [ -r "/proc/$pid/stat" ] && files="$files /proc/$pid/stat"
            [ -r "/proc/$pid/smaps_rollup" ] && files="$files /proc/$pid/smaps_rollup"
        done
        [ -n "$files" ] || return 0
        # shellcheck disable=SC2086
        awk -v hz="$(ticks)" -v root="$root" '
            FILENAME ~ /smaps_rollup$/ {
                if ($1 == "Pss:") pss += $2
                # The same rollup splits that Pss into what is backed by
                # a file and what is not, which is the page cache
                # attribution M1b asks for and the only version of it
                # that means anything here: the box wide Cached figure
                # on a shared server holds the neighbour files too.
                else if ($1 == "Pss_File:") file += $2
                else if ($1 == "Pss_Anon:") anon += $2
                # The third of the three, and on postgres the largest:
                # shared_buffers is a shmem mapping and is neither of
                # the other two. Left out, the split would not add up
                # to the Pss above it and would read as an error.
                else if ($1 == "Pss_Shmem:") shmem += $2
                # SwapPss and not Swap, divided by the mappers for the
                # same reason Pss is.
                else if ($1 == "SwapPss:") swap += $2
                next
            }
            {
                # The comm field can hold spaces and parentheses, so
                # the fields are counted from the closing paren rather
                # than from the start of the line.
                tail = substr($0, index($0, ") ") + 2)
                n = split(tail, f, " ")
                if (n < 15) next
                user += f[12]; sys += f[13]
                # Major faults, the ones that went to the disk. They are
                # gathered exactly like the cpu counters, live tree plus
                # the root child totals, and they do not overlap for the
                # same reason.
                major += f[10]
                if (FILENAME == "/proc/" root "/stat") {
                    user += f[14]; sys += f[15]; major += f[11]
                }
            }
            END {
                if (pss > 0)
                    printf "%d pss %.2f %.2f mem:%d:%d:%d:%d:%d\n",
                        pss, user / hz, sys / hz, file, anon, shmem, swap, major
            }
        ' $files 2>/dev/null || true
    else
        # No Pss and no cutime, so this is a sum of RSS over the live
        # tree and it counts shared memory once per process. Good
        # enough to watch a trend on a laptop, not good enough for a
        # published number, which is one more reason those runs happen
        # on the servers.
        list=$(echo "$pids" | tr '\n' ',' | sed 's/,$//')
        ps -o rss=,utime=,stime= -p "$list" 2>/dev/null | awk '
            { rss += $1; user += clock($2); sys += clock($3) }
            END { if (rss > 0) printf "%d rss %.2f %.2f\n", rss, user, sys }
            function clock(t,   parts, n, seconds, i) {
                n = split(t, parts, ":")
                seconds = 0
                for (i = 1; i <= n; i++) seconds = seconds * 60 + parts[i]
                return seconds
            }' || echo "0 rss 0.00 0.00"
    fi
}

now() { date +%s; }

# Which block device a path sits on, named the way /proc/diskstats names
# it. The partition is preferred over the whole disk, so a run on one
# filesystem is not credited with the traffic of another filesystem on
# the same spindle, and the whole disk is the fallback for the kernels
# and device mappers that do not give a partition a row of its own.
device() {
    [ -r /proc/diskstats ] || return 0
    node=$(df -P "$1" 2>/dev/null | awk 'NR == 2 { print $1 }')
    case $node in
    /dev/*) ;;
    *) return 0 ;;
    esac
    node=$(readlink -f "$node" 2>/dev/null || printf '%s' "$node")
    name=${node#/dev/}
    parent=$(printf '%s' "$name" | sed 's/p\{0,1\}[0-9]*$//')
    for try in "$name" "$parent"; do
        [ -n "$try" ] || continue
        if awk -v n="$try" '$3 == n { hit = 1 } END { exit !hit }' /proc/diskstats; then
            printf '%s\n' "$try"
            return 0
        fi
    done
}

# The distinct devices behind a comma separated list of paths. Both
# halves of a run can land on one device, which is the usual case, and
# then it is sampled once and not twice.
devices() {
    [ -n "${1:-}" ] || return 0
    found=
    outer=$IFS
    IFS=,
    for path in $1; do
        IFS=$outer
        dev=$(device "$path" || true)
        if [ -n "$dev" ]; then
            case ",$found," in
            *",$dev,"*) ;;
            *) found="${found:+$found,}$dev" ;;
            esac
        fi
        IFS=,
    done
    IFS=$outer
    printf '%s' "$found"
}

# One token per device appended to the sample row, colon separated, so
# the memory and cpu fields keep their positions whether or not a disk
# was asked for and whether one device is being watched or three.
#
# Sectors are 512 bytes here regardless of what the hardware calls a
# sector, which is a kernel interface promise and not a guess.
disk() {
    [ -n "${1:-}" ] || return 0
    [ -r /proc/diskstats ] || return 0
    awk -v want="$1" '
        BEGIN { n = split(want, list, ","); for (i = 1; i <= n; i++) keep[list[i]] = 1 }
        keep[$3] && NF >= 14 {
            printf " %s:%d:%d:%d:%d:%d:%d:%d", $3, $4, $6, $7, $8, $10, $11, $13
        }
    ' /proc/diskstats
}

sample() {
    pid=$1
    out=$2
    interval=${3:-1}
    devs=$(devices "${4:-}")
    while kill -0 "$pid" 2>/dev/null; do
        # Word splitting is the point: four fields out, four set.
        # shellcheck disable=SC2046
        set -- $(snapshot "$pid")
        if [ $# -ge 4 ]; then
            # The fifth token is the memory shape and only Linux has it,
            # so it is appended rather than given a column: the report
            # reads it by its prefix and the four fields in front of it
            # keep their positions on every platform and in every
            # samples file written before this existed.
            printf '%s %s %s %s %s%s%s\n' "$(now)" "$1" "$2" "$3" "$4" \
                "${5:+ $5}" "$(disk "$devs")" >>"$out"
        fi
        sleep "$interval"
    done
}

# One line for a window: the peak and the last memory reading in it,
# and the cpu burned during it. The cpu baseline is the last sample
# before the window rather than the first sample inside it, so the
# second between two phases belongs to one of them instead of to
# neither. Empty windows print nothing rather than zeros, because a
# phase too short to be sampled did not use no memory, it was not
# measured.
report() {
    file=$1
    from=${2:-0}
    to=${3:-9999999999}
    # How many transactions the window served, when the caller knows.
    # M1b wants cpu seconds per thousand transactions within 20% of
    # vanilla, and that is a division nobody should be doing by hand
    # off two lines of a log.
    txns=${4:-0}
    [ -s "$file" ] || return 0
    awk -v from="$from" -v to="$to" -v txns="$txns" '
        # Every device token on the row into one of two snapshots, the
        # baseline or the latest, keyed by device and field. The window
        # is the second minus the first, the same subtraction the cpu
        # counters get and for the same reason: these are counters
        # since boot and a phase owns only its own stretch of them.
        function grab(which,   i, j, n, t) {
            for (i = 6; i <= NF; i++) {
                n = split($i, t, ":")
                if (n < 8) continue
                for (j = 2; j <= 8; j++) d[which, t[1], j] = t[j] + 0
                # Only a device the baseline row had too, since a
                # subtraction against a zero it never held would read
                # as the whole uptime of the box landing in one phase.
                if (which == 0) seen_dev[t[1]] = 1
            }
        }
        # The memory shape token, which is six colon separated fields
        # rather than the eight a device token has, so the two never get
        # confused for one another and an older samples file that has
        # neither simply leaves both unset.
        function shape(which,   i, n, t) {
            for (i = 6; i <= NF; i++) {
                n = split($i, t, ":")
                if (n != 6 || t[1] != "mem") continue
                m[which, "file"] = t[2] + 0
                m[which, "anon"] = t[3] + 0
                m[which, "shmem"] = t[4] + 0
                m[which, "swap"] = t[5] + 0
                m[which, "major"] = t[6] + 0
                shaped[which] = 1
            }
        }
        function mib(sectors) { return sectors * 512 / 1048576 }
        # An older samples file could hold a zero row from before the
        # sampler learned to skip a reading it could not take, and one
        # of those in the wrong place turns a window negative.
        $2 <= 0 { next }
        $1 < from { u0 = $4; s0 = $5; grab(0); shape(0); t0 = $1; seen = 1; next }
        $1 <= to {
            n++
            if ($2 > peak) peak = $2
            last = $2
            kind = $3
            if (n == 1 && !seen) { u0 = $4; s0 = $5; grab(0); shape(0); t0 = $1 }
            if (n == 1) tf = $1
            u1 = $4; s1 = $5; t1 = $1
            grab(1)
            shape(1)
            # Least squares on the memory readings, kept as running sums
            # so an hours long samples file is one pass and not an array.
            # Seconds from the first in window sample rather than epoch
            # seconds, which would square into numbers awk rounds.
            dx = $1 - tf
            sx += dx; sy += $2; sxx += dx * dx; sxy += dx * $2
        }
        END {
            if (n == 0) exit
            printf "  %s peak %.1f MiB, end %.1f MiB, cpu %.1f s user %.1f s sys over %d samples",
                kind, peak / 1024, last / 1024, u1 - u0, s1 - s0, n
            if (txns > 0)
                printf ", %.3f s cpu per 1000 transactions",
                    1000 * ((u1 - u0) + (s1 - s0)) / txns
            printf "\n"
            # What that memory is made of, which is the difference
            # between a footprint and a cache. The file backed half is
            # the page cache this tree is charged for and it comes back
            # under pressure, the anonymous half does not, and a shim
            # budget written against the sum of the two is written
            # against the wrong number.
            if (shaped[1]) {
                printf "  memory at end %.1f MiB file backed, %.1f MiB anonymous, %.1f MiB shared",
                    m[1, "file"] / 1024, m[1, "anon"] / 1024, m[1, "shmem"] / 1024
                if (m[1, "swap"] > 0)
                    printf ", %.1f MiB swapped", m[1, "swap"] / 1024
                if (shaped[0])
                    printf ", %d major faults", m[1, "major"] - m[0, "major"]
                printf "\n"
            }
            # A leak is a slope and not a peak, and it does not show up
            # in a sixty second phase, so the check is on windows long
            # enough to hold one. The fit is over the whole window and
            # is reported as a rate rather than as a verdict: a run that
            # is still filling shared_buffers climbs honestly, and a
            # threshold here would call that a leak.
            span_fit = t1 - tf
            if (n >= 2 && span_fit >= 3600) {
                denom = n * sxx - sx * sx
                if (denom > 0) {
                    per_hour = (n * sxy - sx * sy) / denom * 3600 / 1024
                    printf "  slope over %.1f h: %+.2f MiB per hour", span_fit / 3600, per_hour
                    if (peak > 0)
                        printf ", %+.2f%% of the peak an hour", 100 * per_hour * 1024 / peak
                    printf "\n"
                }
            }
            span = t1 - t0
            for (dev in seen_dev) {
                reads = d[1, dev, 2] - d[0, dev, 2]
                sectors_read = d[1, dev, 3] - d[0, dev, 3]
                read_ms = d[1, dev, 4] - d[0, dev, 4]
                writes = d[1, dev, 5] - d[0, dev, 5]
                sectors_written = d[1, dev, 6] - d[0, dev, 6]
                write_ms = d[1, dev, 7] - d[0, dev, 7]
                busy_ms = d[1, dev, 8] - d[0, dev, 8]
                if (reads + writes <= 0) continue
                printf "  disk %s %.1f MiB read, %.1f MiB written, %d reads, %d writes",
                    dev, mib(sectors_read), mib(sectors_written), reads, writes
                if (span > 0)
                    printf ", %d read and %d write iops",
                        reads / span, writes / span
                printf ", %.2f ms await", (read_ms + write_ms) / (reads + writes)
                if (span > 0)
                    printf ", %.0f%% util", 100 * busy_ms / (span * 1000)
                # Whose disk this is, said once rather than implied: a
                # server with a neighbour on the same device is the
                # normal case and these are its numbers too.
                printf ", whole device\n"
            }
        }' "$file"
}

# Bytes written to the sampled devices during a window, on its own, so
# the caller can divide something by it. Used for write amplification,
# which is device bytes over the wal bytes postgres says it produced,
# and those two numbers live in two different files.
written() {
    file=$1
    from=${2:-0}
    to=${3:-9999999999}
    [ -s "$file" ] || { echo 0; return 0; }
    awk -v from="$from" -v to="$to" '
        function grab(which,   i, n, t) {
            for (i = 6; i <= NF; i++) {
                n = split($i, t, ":")
                if (n < 8) continue
                d[which, t[1]] = t[6] + 0
                if (which == 0) seen_dev[t[1]] = 1
            }
        }
        $2 <= 0 { next }
        $1 < from { grab(0); seen = 1; next }
        $1 <= to {
            n++
            if (n == 1 && !seen) grab(0)
            grab(1)
        }
        END {
            for (dev in seen_dev) total += d[1, dev] - d[0, dev]
            printf "%d\n", total * 512
        }' "$file"
}

case ${1:-} in
--report)
    shift
    [ $# -ge 1 ] || {
        echo "usage: $0 --report <samples-file> [from] [to] [transactions]" >&2
        exit 2
    }
    report "$@"
    ;;
--written)
    shift
    [ $# -ge 1 ] || {
        echo "usage: $0 --written <samples-file> [from] [to]" >&2
        exit 2
    }
    written "$@"
    ;;
"")
    echo "usage: $0 <postmaster-pid> <samples-file> [interval] [paths]" >&2
    exit 2
    ;;
*)
    [ $# -ge 2 ] || {
        echo "usage: $0 <postmaster-pid> <samples-file> [interval] [paths]" >&2
        exit 2
    }
    sample "$@"
    ;;
esac
