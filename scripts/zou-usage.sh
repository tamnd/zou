#!/bin/sh
# What a run costs in memory and cpu, sampled while it happens.
#
# Usage: scripts/zou-usage.sh <postmaster-pid> <samples-file> [interval]
#        scripts/zou-usage.sh --report <samples-file> [from] [to] [transactions]
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
# Cpu is the live tree's own time plus the postmaster's cutime and
# cstime, which hold the children it has already reaped. Both halves
# are needed and they do not overlap: without the child counters a
# backend that connected, worked and went away between two samples
# would not be counted at all, and without the live tree a phase's cpu
# would only appear at the disconnect that ends it.
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
            FILENAME ~ /smaps_rollup$/ { if ($1 == "Pss:") pss += $2; next }
            {
                # The comm field can hold spaces and parentheses, so
                # the fields are counted from the closing paren rather
                # than from the start of the line.
                tail = substr($0, index($0, ") ") + 2)
                n = split(tail, f, " ")
                if (n < 15) next
                user += f[12]; sys += f[13]
                if (FILENAME == "/proc/" root "/stat") { user += f[14]; sys += f[15] }
            }
            END { if (pss > 0) printf "%d pss %.2f %.2f\n", pss, user / hz, sys / hz }
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

sample() {
    pid=$1
    out=$2
    interval=${3:-1}
    while kill -0 "$pid" 2>/dev/null; do
        # Word splitting is the point: four fields out, four set.
        # shellcheck disable=SC2046
        set -- $(snapshot "$pid")
        if [ $# -eq 4 ]; then
            printf '%s %s %s %s %s\n' "$(now)" "$1" "$2" "$3" "$4" >>"$out"
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
        # An older samples file could hold a zero row from before the
        # sampler learned to skip a reading it could not take, and one
        # of those in the wrong place turns a window negative.
        $2 <= 0 { next }
        $1 < from { u0 = $4; s0 = $5; seen = 1; next }
        $1 <= to {
            n++
            if ($2 > peak) peak = $2
            last = $2
            kind = $3
            if (n == 1 && !seen) { u0 = $4; s0 = $5 }
            u1 = $4; s1 = $5
        }
        END {
            if (n == 0) exit
            printf "  %s peak %.1f MiB, end %.1f MiB, cpu %.1f s user %.1f s sys over %d samples",
                kind, peak / 1024, last / 1024, u1 - u0, s1 - s0, n
            if (txns > 0)
                printf ", %.3f s cpu per 1000 transactions",
                    1000 * ((u1 - u0) + (s1 - s0)) / txns
            printf "\n"
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
"")
    echo "usage: $0 <postmaster-pid> <samples-file> [interval]" >&2
    exit 2
    ;;
*)
    [ $# -ge 2 ] || { echo "usage: $0 <postmaster-pid> <samples-file> [interval]" >&2; exit 2; }
    sample "$@"
    ;;
esac
