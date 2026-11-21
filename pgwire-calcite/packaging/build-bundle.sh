#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
# Licensed under the Business Source License 1.1 (see ../LICENSE, ../NOTICE).
#
# Build the pgwire-calcite runtime and one adapter's pack for Linux or macOS (layout:
# src/pgwire_calcite/packs.py), packed as tarballs with their sha256: what pgwire-calcite-release.yml
# and pgwire-adapters-release.yml publish, built from any checkout (Provisa's CI builds
# pgwire-servicenow from a calcite ref this way). The workflows run the same steps themselves; a
# change to one belongs in the other.
#
#   bash pgwire-calcite/packaging/build-bundle.sh <adapter> <variant> <out-dir>
#
#   adapter   a name in packaging/adapters.json (e.g. servicenow)
#   variant   linux-x86_64 | linux-aarch64 | macos-arm64; the runtime runs on the machine it is
#             built on, so the variant must be this machine's
#   out-dir   receives root/runtimes/<version>/ and root/packs/<adapter>/<version>/ (extracted:
#             root/packs/<adapter>/<version>/bin/pgwire-<adapter> is the launcher), and
#             pgwire-calcite-runtime-<version>-<variant>.tar.gz and
#             pgwire-<adapter>-pack-<version>-<variant>.tar.gz, each with its .sha256
#
# The runtime's jars are those every adapter in adapters.json holds, so every adapter's classpath
# is resolved, as the release does. Run from the root of a calcite checkout. Needs JAVA_HOME
# pointing at a JDK 17 and uv on PATH; downloads from Maven, PyPI and python-build-standalone.
#
# Environment:
#   PGWIRE_BUNDLE_VERSION  version of the runtime and the pack, stamped into the runtime's installed
#                          package (the catalog cache's key); default "dev"
#   CATALOG_CACHE_DIR      a directory holding the catalog cache for this model.json and version,
#                          written by scripts/build_catalog_cache.py; shipped in the pack beside the
#                          model. Unset, the pack ships none and an install walks the catalog live on
#                          its first start.
set -euo pipefail

ADAPTER="${1:?usage: build-bundle.sh <adapter> <variant> <out-dir>}"
VARIANT="${2:?usage: build-bundle.sh <adapter> <variant> <out-dir>}"
OUT="${3:?usage: build-bundle.sh <adapter> <variant> <out-dir>}"
VERSION="${PGWIRE_BUNDLE_VERSION:-dev}"
PY_VERSION="3.12"

ROOT="$(pwd)"
PACKAGING="$ROOT/pgwire-calcite/packaging"
[ -f "$PACKAGING/adapters.json" ] || { echo "run from the root of a calcite checkout" >&2; exit 1; }

case "$VARIANT" in
  linux-x86_64|linux-aarch64|macos-arm64) ;;
  *) echo "variant '$VARIANT' is not built by this script (Windows bundles are built by the release workflow)" >&2; exit 1 ;;
esac
# The bundle carries this machine's CPython and JRE, so it is this machine's variant
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) MACHINE=linux-x86_64 ;;
  Linux-aarch64) MACHINE=linux-aarch64 ;;
  Darwin-arm64) MACHINE=macos-arm64 ;;
  *) MACHINE="$(uname -s)-$(uname -m)" ;;
esac
if [ "$VARIANT" != "$MACHINE" ]; then
  echo "variant '$VARIANT' cannot be built on this machine ($MACHINE)" >&2
  exit 1
fi

# The adapter's launcher flags, and every adapter's Gradle module, from the table the workflows read
FLAGS="$(python3 - "$PACKAGING/adapters.json" "$ADAPTER" <<'PY'
import json, sys
for a in json.load(open(sys.argv[1])):
    if a["name"] == sys.argv[2]:
        print(a["flags"])
        break
else:
    sys.exit(f"adapter '{sys.argv[2]}' is not in adapters.json")
PY
)"
# (while-read, not mapfile: macOS ships bash 3.2)
ADAPTERS=()
while IFS= read -r line; do ADAPTERS+=("$line"); done < <(
  python3 -c 'import json,sys; [print(a["name"], a["module"]) for a in json.load(open(sys.argv[1]))]' \
    "$PACKAGING/adapters.json")

mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
RT="$OUT/root/runtimes/$VERSION"
PK="$OUT/root/packs/$ADAPTER/$VERSION"
RUNTIME_NAME="pgwire-calcite-runtime-$VERSION-$VARIANT"
PACK_NAME="pgwire-$ADAPTER-pack-$VERSION-$VARIANT"
rm -rf "${OUT:?}/root" "${OUT:?}/.jars" "${OUT:?}/$RUNTIME_NAME".tar.gz* "${OUT:?}/$PACK_NAME".tar.gz*

# ---- one adapter's jars: its runtime classpath, the pgwire Arrow bridge, the vendor and UDF jars
adapter_jars() {
  local module="$1" dest="$2"
  mkdir -p "$dest"
  ./gradlew ":$module:jar" :core:jar :linq4j:jar --no-daemon --console=plain -q
  ./gradlew ":$module:pgwPrintCp" --init-script pgwire-calcite/scripts/print-calcite-classpath.gradle \
    --no-daemon --console=plain -q
  for j in $(tr ':' '\n' < /tmp/pgw-file-cp.txt | grep '\.jar$'); do cp -n "$j" "$dest/" || true; done
  for j in $(tr ':' '\n' < /tmp/pgw-arrow-cp.txt | grep '\.jar$'); do cp -n "$j" "$dest/" || true; done
  # `unzip -l a.jar b.jar ...` lists only a.jar and treats the rest as member patterns, so each
  # jar is visited on its own
  for c in org.apache.arrow.memory.RootAllocator com.google.flatbuffers.Table org.duckdb.DuckDBDriver; do
    local found=0
    for j in "$dest"/*.jar; do
      unzip -l "$j" 2>/dev/null | grep -q "$(echo "$c" | tr . /).class" && { found=1; break; }
    done
    [ "$found" = 1 ] || { echo "$module jars are missing $c" >&2; exit 1; }
  done
  # the adapter module's own output is a classes dir on the classpath; use its built jar
  cp -n "$module"/build/libs/*.jar "$dest/" 2>/dev/null || true
  # Two independently resolved classpaths; `cp -n` dedupes only by file name, so keep the highest
  # version of each artifact (2026-09-16: two jackson-core jars broke every pgwire-govdata start)
  python3 pgwire-calcite/scripts/dedupe_jars.py "$dest"
  cp "$OUT/.jars/vendor/"*.jar "$dest/"
  python3 pgwire-calcite/scripts/dedupe_jars.py "$dest"
}

# ---- the runtime: CPython + server, JRE, launcher, the jars every adapter holds
build_runtime() {
  ./gradlew pgwArrowCp --init-script pgwire-calcite/scripts/print-calcite-classpath.gradle \
    --no-daemon --console=plain -q
  # vendor/jars, with a fresh UDF jar compiled into the build instead of the checked-in one, so
  # the checkout is not modified
  mkdir -p "$OUT/.jars/vendor"
  cp pgwire-calcite/vendor/jars/*.jar "$OUT/.jars/vendor/"
  (cd pgwire-calcite && PGWIRE_UDF_OUT="$OUT/.jars/vendor/pgwire-vecdist.jar" bash packaging/build-udf.sh \
    && bash packaging/build-launcher.sh)
  local dirs=() name module
  for entry in "${ADAPTERS[@]}"; do
    read -r name module <<<"$entry"
    adapter_jars "$module" "$OUT/.jars/$name"
    dirs+=("$OUT/.jars/$name")
  done
  mkdir -p "$RT"
  python3 "$PACKAGING/runtime_packs.py" common-jars "$RT/jars" "${dirs[@]}"
  cp pgwire-calcite/packaging/pgwire-launcher.jar "$RT/"

  # The interpreter goes INTO the runtime, not uv's global location, so the tarball is
  # self-contained: an interpreter outside it would leave cpython/bin/python an absolute symlink
  # to this machine's path
  (cd pgwire-calcite && uv python install --install-dir "$RT/python" "$PY_VERSION")
  find "$RT/python" -maxdepth 1 -type l -delete   # uv's version alias points outside the runtime
  rm -rf "${RT:?}/python/.lock" "${RT:?}/python/.temp" "${RT:?}/python/.gitignore"
  local pybin
  pybin="$(find "$RT/python" -maxdepth 3 -type f -name "python${PY_VERSION}")"
  [ -n "$pybin" ] || { echo "runtime interpreter not found under $RT/python" >&2; exit 1; }
  uv venv --python "$pybin" "$RT/cpython"
  # --no-editable: vendor/buenavista is an editable source for local development; ship a real
  # install. Adapter-only packages (govdata's embedder) go in that adapter's pack.
  (cd pgwire-calcite && uv pip install --python "$RT/cpython" --no-editable --override packaging/uv-overrides.txt ./vendor/buenavista ".[oidc]")
  # The engine release this runtime belongs to, in its installed package: the catalog cache is
  # keyed by it (catalog_cache_key.engine_version)
  local pkg
  pkg="$("$RT/cpython/bin/python" -c 'import os, pgwire_calcite; print(os.path.dirname(pgwire_calcite.__file__))')"
  echo "$VERSION" > "$pkg/ENGINE_VERSION"
  # `uv venv` makes cpython/bin/python an absolute symlink; make it relative so the runtime can be
  # moved anywhere. CPython 3.11+ derives its prefix from the link's real path.
  python3 - "$pybin" "$RT/cpython/bin" <<'PY'
import os, sys
target = os.path.realpath(sys.argv[1])
venv_bin = sys.argv[2]
link = os.path.join(venv_bin, "python")
rel = os.path.relpath(target, os.path.realpath(venv_bin))
os.remove(link)
os.symlink(rel, link)
print(f"bin/python -> {rel}")
PY
  # Every JDK module: this runtime starts the launcher and, through JPype, the Calcite JVM that
  # loads any adapter's whole classpath
  "$JAVA_HOME/bin/jlink" --add-modules ALL-MODULE-PATH \
    --strip-debug --no-header-files --no-man-pages --compress=2 --output "$RT/jre"
  python3 "$PACKAGING/runtime_packs.py" runtime "$RT" "$VERSION" "$VARIANT"
}

# ---- the adapter's pack: its other jars, its model, its Python extras, its launcher
build_pack() {
  python3 "$PACKAGING/runtime_packs.py" pack-jars "$RT/jars" "$OUT/.jars/$ADAPTER" "$PK/jars"
  mkdir -p "$PK/model"
  cp "pgwire-$ADAPTER/model.json" "$PK/model/model.json"
  if [ "$ADAPTER" = "govdata" ]; then
    # EMBED() needs fastembed, which only govdata uses: installed into the pack with the
    # runtime's interpreter, keeping only what the runtime lacks; the model is downloaded here so
    # the pack needs no network at run time
    local reqs=() line
    while IFS= read -r line; do reqs+=("$line"); done < <(
      python3 -c "import tomllib; print('\n'.join(tomllib.load(open('pgwire-calcite/pyproject.toml','rb'))['project']['optional-dependencies']['embed']))")
    python3 "$PACKAGING/runtime_packs.py" pack-extras "$RT/cpython/bin/python" "$PK" "${reqs[@]}"
    PYTHONPATH="$PK/site-packages" "$RT/cpython/bin/python" -c "
from fastembed import TextEmbedding
TextEmbedding(model_name='snowflake/snowflake-arctic-embed-xs', cache_dir='$PK/model/embedder')
print('embedder model cached')
"
  fi
  # A catalog cache is keyed by model.json and the engine release, so none is committed: one walked
  # by scripts/build_catalog_cache.py with this build's jars and version, in CATALOG_CACHE_DIR,
  # ships beside the model. A directory that holds no cache under this build's key fails the build.
  if [ -n "${CATALOG_CACHE_DIR:-}" ]; then
    local key expected
    key="$(PYTHONPATH=pgwire-calcite/src python3 -c 'import sys; from pgwire_calcite.catalog_cache_key import cache_key; print(cache_key(sys.argv[1], sys.argv[2]))' \
      "pgwire-$ADAPTER/model.json" "$VERSION")"
    expected="$CATALOG_CACHE_DIR/catalog-cache-${key}.pkl"
    [ -f "$expected" ] || { echo "no catalog cache $expected for this model.json and engine $VERSION" >&2; exit 1; }
    if [ "$ADAPTER" = "govdata" ]; then
      # The check imports PyYAML, which the runner's system python lacks; uv supplies it for this run.
      PYTHONPATH=pgwire-calcite/src uv run --no-project --with pyyaml python pgwire-calcite/scripts/check_catalog_cache_coverage.py \
        "$expected" pgwire-govdata/unmaterialized-tables.txt govdata/src/main/resources/*/*-schema.yaml
    fi
    cp "$expected" "$PK/model/$(basename "$expected")"
  fi
  # --database names the reported catalog after the adapter; the caller's arguments follow the
  # pack's flags and win
  # shellcheck disable=SC2086
  python3 "$PACKAGING/runtime_packs.py" pack "$PK" "$ADAPTER" "$VERSION" "$VARIANT" \
    --database "$ADAPTER" $FLAGS
}

pack_tarball() {
  local name="$1" part="$2"
  (cd "$OUT" && tar -czf "$name.tar.gz" -C root "$part")
  # `command -v` only for its status, so its own output cannot reach the hash file
  (cd "$OUT" && (command -v sha256sum >/dev/null 2>&1 && sha256sum "$name.tar.gz" || shasum -a 256 "$name.tar.gz") \
    | awk '{print $1}' > "$name.tar.gz.sha256")
  echo "built $OUT/$name.tar.gz ($(cat "$OUT/$name.tar.gz.sha256"))"
}

build_runtime
build_pack
pack_tarball "$RUNTIME_NAME" runtimes
pack_tarball "$PACK_NAME" packs
rm -rf "${OUT:?}/.jars"
echo "RUNTIME_NAME=$RUNTIME_NAME"
echo "PACK_NAME=$PACK_NAME"
echo "LAUNCHER=$PK/bin/pgwire-$ADAPTER"
