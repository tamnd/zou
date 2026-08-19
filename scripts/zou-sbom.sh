#!/usr/bin/env bash
# What is inside a zou bundle, as a CycloneDX document.
#
# A release ships a binary with a javascript engine linked into it, a
# patched Postgres, and pgvector. That is several hundred rust crates
# and two C projects, and the only honest answer to "does this ship the
# thing in today's advisory" is a list. This writes one.
#
#   scripts/zou-sbom.sh                                  # to stdout
#   scripts/zou-sbom.sh --out dist/zou-linux-x64.cdx.json
#   scripts/zou-sbom.sh --version 1.2.3 --tarball dist/zou-linux-x64.tar.gz
#
# The rust half comes out of cargo's own resolver for the host target
# and the features a release is built with, so it is the graph that was
# compiled rather than everything the manifests mention. The C half
# comes out of the vendored trees: the version each one calls itself,
# and the commit this repository pins it at.
#
# Build time crates are in the document and marked as such. A proc
# macro is not in the shipped binary, and a build script that runs on
# the machine that made it is still worth knowing about.
#
# Exits non zero if what it wrote is not a document it would accept,
# which is most of the point of writing it here rather than in a
# workflow: it can be run and read.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out=-
version=""
tarball=""
features="zou-deno/isolate"

while [ $# -gt 0 ]; do
  case "$1" in
    --out) out="$2"; shift 2 ;;
    --version) version="$2"; shift 2 ;;
    --tarball) tarball="$2"; shift 2 ;;
    --features) features="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

host="$(rustc -vV | sed -n 's/^host: //p')"
metadata="$(mktemp)"
built="$(mktemp)"
trap 'rm -f "$metadata" "$built"' EXIT
cargo metadata --format-version 1 --locked \
  --filter-platform "$host" \
  --features "$features" \
  --manifest-path "$root/crates/zou/Cargo.toml" > "$metadata"

# The two C projects. Each version is the one that tree calls itself,
# read out of the file the project maintains, and each commit is what
# this repository has pinned in its index, which is what a build checks
# out. Missing submodules are not fatal: a document that says the rust
# half and says plainly that it could not read the vendored trees is
# more use than no document, and the check at the end refuses to write
# one for a release that way.
pg_version="$(sed -n "s/^  version: '\\(.*\\)',$/\\1/p" "$root/vendor/postgres/meson.build" 2>/dev/null | head -1)"
vector_version="$(sed -n "s/^default_version = '\\(.*\\)'$/\\1/p" "$root/vendor/pgvector/vector.control" 2>/dev/null | head -1)"
pins="$(git -C "$root" ls-tree HEAD vendor/postgres vendor/pgvector 2>/dev/null || true)"
pg_commit="$(printf '%s\n' "$pins" | sed -n 's/^160000 commit \([0-9a-f]*\).*vendor\/postgres$/\1/p')"
vector_commit="$(printf '%s\n' "$pins" | sed -n 's/^160000 commit \([0-9a-f]*\).*vendor\/pgvector$/\1/p')"

sha=""
if [ -n "$tarball" ]; then
  if command -v sha256sum >/dev/null; then
    sha="$(sha256sum "$tarball" | cut -d' ' -f1)"
  else
    sha="$(shasum -a 256 "$tarball" | cut -d' ' -f1)"
  fi
fi

ZOU_METADATA="$metadata" \
  ZOU_VERSION="$version" \
  ZOU_HOST="$host" \
  ZOU_FEATURES="$features" \
  ZOU_PG_VERSION="$pg_version" \
  ZOU_PG_COMMIT="$pg_commit" \
  ZOU_VECTOR_VERSION="$vector_version" \
  ZOU_VECTOR_COMMIT="$vector_commit" \
  ZOU_TARBALL="$(basename "${tarball:-}")" \
  ZOU_TARBALL_SHA="$sha" \
  python3 - > "$built" <<'PYTHON'
import hashlib
import json
import os
import uuid

meta = json.load(open(os.environ["ZOU_METADATA"]))
packages = {p["id"]: p for p in meta["packages"]}
nodes = {n["id"]: n for n in meta["resolve"]["nodes"]}
root_id = meta["resolve"]["root"]

# Walk the graph the compiler walks. Dev dependencies are not in the
# artifact and are not here; build dependencies are, marked, because a
# build script runs on the machine that made the release even though
# nothing of it is in the binary. Anything reached both ways is normal,
# since the stronger claim is the one that matters.
kinds = {}


def walk(node_id, building):
    known = kinds.get(node_id)
    if known == "normal" or (known == "build" and building):
        return
    kinds[node_id] = "build" if building else "normal"
    for dep in nodes[node_id]["deps"]:
        for kind in dep["dep_kinds"]:
            if kind["kind"] == "dev":
                continue
            walk(dep["pkg"], building or kind["kind"] == "build")


walk(root_id, False)
del kinds[root_id]

def purl(pkg):
    return f"pkg:cargo/{pkg['name']}@{pkg['version']}"


def component(pkg, kind):
    out = {
        "type": "library",
        "bom-ref": purl(pkg),
        "name": pkg["name"],
        "version": pkg["version"],
        "purl": purl(pkg),
        "properties": [{"name": "zou:dependency", "value": kind}],
    }
    if pkg.get("description"):
        out["description"] = pkg["description"]
    if pkg.get("license"):
        out["licenses"] = [{"expression": pkg["license"]}]
    refs = []
    if pkg.get("repository"):
        refs.append({"type": "vcs", "url": pkg["repository"]})
    source = pkg.get("source") or ""
    if source.startswith("registry+"):
        refs.append({"type": "distribution", "url": source[len("registry+"):]})
    if refs:
        out["externalReferences"] = refs
    return out


components = [
    component(packages[i], kind)
    for i, kind in sorted(kinds.items(), key=lambda pair: packages[pair[0]]["name"])
]

# The C half. These are not cargo packages and do not get a cargo purl,
# and each one carries the commit as well as the version, because the
# tree that is built is the pinned commit with this repository's own
# patch series on top of it and a version number alone would not say
# which.
def native(name, version, commit, url, licence, description):
    if not version:
        return None
    out = {
        "type": "library",
        "bom-ref": f"pkg:generic/{name}@{version}",
        "name": name,
        "version": version,
        "description": description,
        "purl": f"pkg:generic/{name}@{version}",
        "licenses": [{"expression": licence}],
        "externalReferences": [{"type": "vcs", "url": url}],
        "properties": [{"name": "zou:dependency", "value": "vendored"}],
    }
    if commit:
        out["properties"].append({"name": "zou:commit", "value": commit})
    return out


native_components = [
    native(
        "postgresql",
        os.environ["ZOU_PG_VERSION"],
        os.environ["ZOU_PG_COMMIT"],
        "https://github.com/postgres/postgres",
        "PostgreSQL",
        "the postmaster the bundle starts, patched with this repository's storage manager series",
    ),
    native(
        "pgvector",
        os.environ["ZOU_VECTOR_VERSION"],
        os.environ["ZOU_VECTOR_COMMIT"],
        "https://github.com/pgvector/pgvector",
        "PostgreSQL",
        "the vector type and its indexes, built against the bundled postgres",
    ),
]
components.extend(c for c in native_components if c)


def dependencies():
    out = []
    for node_id in [root_id] + sorted(kinds, key=lambda i: packages[i]["name"]):
        depends = sorted(
            {
                purl(packages[dep["pkg"]])
                for dep in nodes[node_id]["deps"]
                if dep["pkg"] in kinds
                and any(k["kind"] != "dev" for k in dep["dep_kinds"])
            }
        )
        ref = "zou" if node_id == root_id else purl(packages[node_id])
        out.append({"ref": ref, "dependsOn": depends})
    return out


version = os.environ["ZOU_VERSION"] or packages[root_id]["version"]
me = {
    "type": "application",
    "bom-ref": "zou",
    "name": "zou",
    "version": version,
    "description": packages[root_id].get("description", ""),
    "licenses": [{"expression": packages[root_id].get("license", "Apache-2.0")}],
    "purl": f"pkg:cargo/zou@{version}",
    "externalReferences": [{"type": "vcs", "url": "https://github.com/tamnd/zou"}],
    "properties": [
        {"name": "zou:target", "value": os.environ["ZOU_HOST"]},
        {"name": "zou:features", "value": os.environ["ZOU_FEATURES"]},
    ],
}
if os.environ["ZOU_TARBALL_SHA"]:
    me["properties"].append({"name": "zou:tarball", "value": os.environ["ZOU_TARBALL"]})
    me["hashes"] = [{"alg": "SHA-256", "content": os.environ["ZOU_TARBALL_SHA"]}]

document = {
    "bomFormat": "CycloneDX",
    "specVersion": "1.5",
    "version": 1,
    "metadata": {
        "tools": [{"vendor": "zou", "name": "scripts/zou-sbom.sh"}],
        "component": me,
    },
    "components": components,
    "dependencies": dependencies(),
}

# A serial number that is the same for the same document, so two runs
# on the same tree produce the same file and a diff between two
# releases is only what changed. A timestamp would do the opposite,
# which is why there is not one.
seed = json.dumps(document, sort_keys=True, separators=(",", ":"))
digest = bytearray(hashlib.sha256(seed.encode()).digest()[:16])
# A uuid with the version and variant bits a uuid is supposed to have,
# so a validator that checks the shape does not reject a serial number
# that is otherwise a perfectly good hash.
digest[6] = (digest[6] & 0x0F) | 0x40
digest[8] = (digest[8] & 0x3F) | 0x80
document["serialNumber"] = "urn:uuid:" + str(uuid.UUID(bytes=bytes(digest)))
print(json.dumps(document, indent=2))
PYTHON

# Read back what was written and refuse to hand out a document that
# would tell somebody the wrong thing. A missing vendored tree is the
# case this really catches: a bundle is mostly postgres, and an SBOM
# that lists four hundred rust crates and no postmaster reads as
# complete while being wrong about the largest thing in the artifact.
python3 - "$built" <<'PYTHON'
import json
import re
import sys

document = json.load(open(sys.argv[1]))
components = document["components"]
names = {c["name"] for c in components}
problems = []
if document.get("bomFormat") != "CycloneDX":
    problems.append("it is not a cyclonedx document")
if not re.fullmatch(r"urn:uuid:[0-9a-f-]{36}", document.get("serialNumber", "")):
    problems.append("the serial number is not a urn:uuid")
if len(components) < 100:
    problems.append(f"only {len(components)} components, the graph did not resolve")
for wanted in ("postgresql", "pgvector"):
    if wanted not in names:
        problems.append(f"no {wanted}: run make pg-init so the vendored trees are there")
for wanted in ("tokio", "axum", "deno_core"):
    if wanted not in names:
        problems.append(f"no {wanted}, so this is not the graph a release is built from")
bare = [c.get("name", "?") for c in components if not all(c.get(f) for f in ("name", "version", "bom-ref", "purl", "licenses"))]
if bare:
    problems.append(f"{len(bare)} components are missing a name, version, ref, purl or licence: {bare[:5]}")
# A dependency graph that points at something not in the document is
# worse than no graph, because a reader following it finds nothing and
# has no way to tell that from a component with no dependencies.
refs = {c["bom-ref"] for c in components} | {document["metadata"]["component"]["bom-ref"]}
dangling = {r for entry in document["dependencies"] for r in [entry["ref"]] + entry["dependsOn"] if r not in refs}
if dangling:
    problems.append(f"{len(dangling)} dependency refs name nothing in the document: {sorted(dangling)[:5]}")
leaked = [c for c in components if "file://" in json.dumps(c)]
if leaked:
    problems.append(f"{len(leaked)} components carry a path from the machine that built them")
if problems:
    print("this sbom is not one to publish:", file=sys.stderr)
    for problem in problems:
        print(f"  {problem}", file=sys.stderr)
    sys.exit(1)
PYTHON

if [ "$out" = - ]; then
  cat "$built"
  exit 0
fi

mkdir -p "$(dirname "$out")"
cp "$built" "$out"
echo "wrote $out"
python3 - "$out" <<'PYTHON'
import collections
import json
import sys

document = json.load(open(sys.argv[1]))
counted = collections.Counter(
    p["value"]
    for c in document["components"]
    for p in c.get("properties", [])
    if p["name"] == "zou:dependency"
)
print(document["serialNumber"])
print(", ".join(f"{n} {kind}" for kind, n in sorted(counted.items())))
for c in document["components"]:
    if c["name"] in ("postgresql", "pgvector"):
        print(f"  {c['name']} {c['version']}")
PYTHON
