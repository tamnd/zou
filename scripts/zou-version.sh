#!/bin/sh
# Put one version number into every manifest that goes out under this
# project's name.
#
# The tag is the version. Nothing in the tree is authored: what is
# committed is a placeholder, the same one everywhere, and a release
# stamps the tag over it in each job that builds or publishes something.
#
# It is one script rather than a sed in each job because the release
# used to stamp three of the five manifests and miss two, so tagging
# v1.0.0 would have published zou-cli at 1.0.0 around a binary that
# answered `zou --version` with 0.0.1. A job that forgets to call this
# is a visible missing line, where a job that has its own sed is a line
# nobody notices is absent.
#
# usage:
#   scripts/zou-version.sh 1.2.3   put that version in every manifest
#   scripts/zou-version.sh v1.2.3  the same, a tag name is accepted
#   scripts/zou-version.sh --list  print the manifests, one per line
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
list=$(grep -v '^#' "$root/packaging/versioned-manifests.txt" | grep -v '^$')

if [ "${1:-}" = --list ]; then
	printf '%s\n' "$list"
	exit 0
fi

version=${1:-}
if [ -z "$version" ]; then
	echo "usage: zou-version.sh <version>|--list" >&2
	exit 2
fi
version=${version#v}

# A version that is not three numbers is a tag somebody typed wrong, and
# the place to find that out is here rather than on a registry that does
# not take the name back.
case "$version" in
*[!0-9.]* | *..* | .* | *.) echo "not a version: $version" >&2; exit 2 ;;
esac
case "$version" in
*.*.*) ;;
*) echo "not a version: $version" >&2; exit 2 ;;
esac

for manifest in $list; do
	path="$root/$manifest"
	case "$manifest" in
	*.toml) expr="s/^version = \".*\"$/version = \"$version\"/" ;;
	*.json) expr="s/^\\(  \"version\": \\)\".*\",$/\\1\"$version\",/" ;;
	*) echo "no rule for $manifest" >&2; exit 1 ;;
	esac
	tmp=$(mktemp)
	sed "$expr" "$path" >"$tmp"
	mv "$tmp" "$path"
	# A sed that matched nothing leaves a file that still parses and
	# still builds, and the placeholder reaches a registry. So the
	# write is read back rather than assumed.
	case "$manifest" in
	*.toml) grep -q "^version = \"$version\"$" "$path" || { echo "$manifest still has no version $version in it" >&2; exit 1; } ;;
	*.json) grep -q "^  \"version\": \"$version\",$" "$path" || { echo "$manifest still has no version $version in it" >&2; exit 1; } ;;
	esac
	echo "$manifest $version"
done
