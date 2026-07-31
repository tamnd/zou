#!/usr/bin/env sh
# Resets vendor/postgres to the pinned commit and applies the patch
# series in patches/ in filename order. The result is a pure function of
# the pinned commit and the series, which is what makes rebuilds
# reproducible.
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)
pg="$root/vendor/postgres"

if [ ! -f "$pg/configure" ]; then
    echo "vendor/postgres is not checked out, run make pg-init first" >&2
    exit 1
fi

pinned=$(git -C "$root" ls-tree HEAD vendor/postgres | awk '{print $3}')

# Refuse to clobber uncommitted work in the submodule. Turn edits into a
# patch first (see docs/postgres.md) or pass FORCE=1 to discard them.
# Only tracked files count: untracked junk like .DS_Store gets cleaned
# below, so brand new files in a patch draft must be git add -N'd to be
# protected here.
if [ "${FORCE:-0}" != "1" ] && [ -n "$(git -C "$pg" status --porcelain --untracked-files=no)" ]; then
    echo "vendor/postgres has local changes." >&2
    echo "Export them with git diff into patches/, or rerun with FORCE=1 to discard." >&2
    exit 1
fi

git -C "$pg" checkout --quiet --detach "$pinned"
git -C "$pg" reset --hard --quiet "$pinned"
git -C "$pg" clean -fdq

applied=0
for p in "$root"/patches/*.patch; do
    [ -e "$p" ] || break
    echo "applying $(basename "$p")"
    git -C "$pg" apply --verbose "$p"
    applied=$((applied + 1))
done

if [ "$applied" -eq 0 ]; then
    echo "no patches in series, tree is stock at $pinned"
else
    echo "applied $applied patch(es) on $pinned"
fi
