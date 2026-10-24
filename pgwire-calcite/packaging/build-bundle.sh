#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
# Licensed under the Business Source License 1.1 (see ../LICENSE, ../NOTICE).
#
# Build one pgwire-<adapter> bundle for Linux or macOS: the adapter's jars, a standalone CPython
# with the pgwire server installed, a jlinked JRE, the adapter's model and its launcher, packed
# as a tarball with its sha256: the bundle pgwire-adapters-release.yml publishes, built from any
# checkout (Provisa's CI builds pgwire-servicenow from a calcite ref this way). The release
# workflow runs the same steps itself; a change to one belongs in the other.
#
#   bash pgwire-calcite/packaging/build-bundle.sh <adapter> <variant> <out-dir>
#
#   adapter   a name in packaging/adapters.json (e.g. servicenow)
#   variant   linux-x86_64 | linux-arm64 | macos-arm64; the bundle runs on the machine it is
#             built on, so the variant must be this machine's
#   out-dir   receives pgwire-<adapter>-<version>-<variant>/ (bin/pgwire-<adapter> is the
#             launcher) and pgwire-<adapter>-<version>-<variant>.tar.gz(.sha256)
#
# Run from the root of a calcite checkout. Needs JAVA_HOME pointing at a JDK 17 and uv on PATH;
# downloads from Maven, PyPI and python-build-standalone.
#
# Environment:
#   PGWIRE_BUNDLE_VERSION  version in the bundle's name, and the engine release stamped into its
#                          installed package (the catalog cache's key); default "dev"
#   CATALOG_CACHE_DIR      a directory holding the catalog cache for this model.json and version,
#                          written by scripts/build_catalog_cache.py; shipped beside the model
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
  linux-x86_64|linux-arm64|macos-arm64) ;;
  *) echo "variant '$VARIANT' is not built by this script (Windows bundles are built by the release workflow)" >&2; exit 1 ;;
esac
# The bundle carries this machine's CPython and JRE, so it is this machine's variant
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) MACHINE=linux-x86_64 ;;
  Linux-aarch64) MACHINE=linux-arm64 ;;
  Darwin-arm64) MACHINE=macos-arm64 ;;
  *) MACHINE="$(uname -s)-$(uname -m)" ;;
esac
if [ "$VARIANT" != "$MACHINE" ]; then
  echo "variant '$VARIANT' cannot be built on this machine ($MACHINE)" >&2
  exit 1
fi

# The adapter's Gradle module and launcher flags, from the one table the workflow also reads
read -r MODULE FLAGS < <(python3 - "$PACKAGING/adapters.json" "$ADAPTER" <<'PY'
import json, sys
for a in json.load(open(sys.argv[1])):
    if a["name"] == sys.argv[2]:
        print(a["module"], a["flags"])
        break
else:
    sys.exit(f"adapter '{sys.argv[2]}' is not in adapters.json")
PY
)

mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"

# ---- jars: the adapter's runtime classpath, the pgwire Arrow bridge, the UDF jar, the launcher
build_jars() {
  local dest="$1"
  mkdir -p "$dest/jars"
  ./gradlew ":$MODULE:jar" :core:jar :linq4j:jar --no-daemon --console=plain -q
  ./gradlew ":$MODULE:pgwPrintCp" --init-script pgwire-calcite/scripts/print-calcite-classpath.gradle \
    --no-daemon --console=plain -q
  ./gradlew pgwArrowCp --init-script pgwire-calcite/scripts/print-calcite-classpath.gradle \
    --no-daemon --console=plain -q
  # every real .jar on the adapter's runtime classpath, and the pgwire server's own Arrow bridge
  # jars, which every adapter bundle needs whether or not the adapter pulls Arrow transitively
  for j in $(tr ':' '\n' < /tmp/pgw-file-cp.txt | grep '\.jar$'); do cp -n "$j" "$dest/jars/" || true; done
  for j in $(tr ':' '\n' < /tmp/pgw-arrow-cp.txt | grep '\.jar$'); do cp -n "$j" "$dest/jars/" || true; done
  # `unzip -l a.jar b.jar ...` lists only a.jar and treats the rest as member patterns, so each
  # jar is visited on its own
  for c in org.apache.arrow.memory.RootAllocator com.google.flatbuffers.Table org.duckdb.DuckDBDriver; do
    local found=0
    for j in "$dest"/jars/*.jar; do
      unzip -l "$j" 2>/dev/null | grep -q "$(echo "$c" | tr . /).class" && { found=1; break; }
    done
    [ "$found" = 1 ] || { echo "bundle is missing $c" >&2; exit 1; }
  done
  # the adapter module's own output is a classes dir on the classpath; use its built jar
  cp -n "$MODULE"/build/libs/*.jar "$dest/jars/" 2>/dev/null || true
  # The jars come from independently resolved classpaths; `cp -n` dedupes only by file name, so
  # two versions of one artifact can both land here (seen 2026-09-16: two jackson-core jars,
  # a NoSuchMethodError on every pgwire-govdata start). Keep the highest version of each.
  python3 pgwire-calcite/scripts/dedupe_jars.py "$dest/jars"
  # vendor/jars is a third jar source. The UDF jar in it is checked in; this build compiles a
  # fresh one into the bundle instead, so the checkout is not modified
  cp pgwire-calcite/vendor/jars/*.jar "$dest/jars/"
  (cd pgwire-calcite && PGWIRE_UDF_OUT="$dest/jars/pgwire-vecdist.jar" bash packaging/build-udf.sh \
    && bash packaging/build-launcher.sh)
  # the same pass again now that vendor/jars is merged in
  python3 pgwire-calcite/scripts/dedupe_jars.py "$dest/jars"
  cp pgwire-calcite/packaging/pgwire-launcher.jar "$dest/"
  echo "bundled $(ls "$dest/jars" | wc -l | tr -d ' ') jars for $ADAPTER"
}

# ---- stage: CPython + server, JRE, model, launcher; then pack
stage_bundle() {
  local jars="$1"
  local name="pgwire-$ADAPTER-$VERSION-$VARIANT"
  local bundle="$OUT/$name"
  rm -rf "$bundle" "$OUT/$name.tar.gz" "$OUT/$name.tar.gz.sha256"
  mkdir -p "$bundle"
  cp -R "$jars/jars" "$bundle/jars"
  cp "$jars/pgwire-launcher.jar" "$bundle/"

  # The interpreter goes INTO the bundle, not uv's global location, so the tarball is
  # self-contained: an interpreter outside it would leave cpython/bin/python an absolute symlink
  # to this machine's path
  (cd pgwire-calcite && uv python install --install-dir "$bundle/python" "$PY_VERSION")
  find "$bundle/python" -maxdepth 1 -type l -delete   # uv's version alias points outside the bundle
  rm -rf "$bundle/python/.lock" "$bundle/python/.temp" "$bundle/python/.gitignore"
  local pybin
  pybin="$(find "$bundle/python" -maxdepth 3 -type f -name "python${PY_VERSION}")"
  [ -n "$pybin" ] || { echo "bundled interpreter not found under $bundle/python" >&2; exit 1; }
  uv venv --python "$pybin" "$bundle/cpython"
  # --no-editable: vendor/buenavista is an editable source for local development; ship a real
  # install. EMBED() needs fastembed, and only govdata has any use for it.
  local extras=".[oidc]"
  [ "$ADAPTER" = "govdata" ] && extras=".[oidc,embed]"
  (cd pgwire-calcite && uv pip install --python "$bundle/cpython" --no-editable --override packaging/uv-overrides.txt ./vendor/buenavista "$extras")
  # The engine release this bundle belongs to, in the installed package: the catalog cache is
  # keyed by it (catalog_populate.engine_version)
  local pkg
  pkg="$("$bundle/cpython/bin/python" -c 'import os, pgwire_calcite; print(os.path.dirname(pgwire_calcite.__file__))')"
  echo "$VERSION" > "$pkg/ENGINE_VERSION"
  # `uv venv` makes cpython/bin/python an absolute symlink; make it relative so the bundle can be
  # moved anywhere. CPython 3.11+ derives its prefix from the link's real path, so pyvenv.cfg's
  # build-machine `home` line is harmless.
  python3 - "$pybin" "$bundle/cpython/bin" <<'PY'
import os, sys
target = os.path.realpath(sys.argv[1])
venv_bin = sys.argv[2]
link = os.path.join(venv_bin, "python")
rel = os.path.relpath(target, os.path.realpath(venv_bin))
os.remove(link)
os.symlink(rel, link)
print(f"bin/python -> {rel}")
PY

  if [ "$ADAPTER" = "govdata" ]; then
    # The EMBED() model, downloaded here so the bundle needs no network at run time
    "$bundle/cpython/bin/python" -c "
from fastembed import TextEmbedding
TextEmbedding(model_name='snowflake/snowflake-arctic-embed-xs', cache_dir='$bundle/model/embedder')
print('embedder model cached')
"
  fi

  # Every JDK module: this runtime starts the launcher and, through JPype, the Calcite JVM that
  # loads the adapter's whole classpath
  "$JAVA_HOME/bin/jlink" --add-modules ALL-MODULE-PATH \
    --strip-debug --no-header-files --no-man-pages --compress=2 --output "$bundle/jre"

  mkdir -p "$bundle/model" "$bundle/bin"
  cp "pgwire-$ADAPTER/model.json" "$bundle/model/model.json"
  # A catalog cache is keyed by model.json and the engine release, so none is committed: one walked
  # by scripts/build_catalog_cache.py with this build's jars and PGWIRE_BUNDLE_VERSION as the stamp,
  # in CATALOG_CACHE_DIR, ships beside the model; without it, an install walks the catalog live on
  # its first start. A directory that holds no cache under this build's key fails the build.
  if [ -n "${CATALOG_CACHE_DIR:-}" ]; then
    local key expected
    key="$(PYTHONPATH=pgwire-calcite/src python3 -c 'import sys; from pgwire_calcite.catalog_populate import cache_key; print(cache_key(sys.argv[1], sys.argv[2]))' \
      "pgwire-$ADAPTER/model.json" "$VERSION")"
    expected="$CATALOG_CACHE_DIR/catalog-cache-${key}.pkl"
    [ -f "$expected" ] || { echo "no catalog cache $expected for this model.json and engine $VERSION" >&2; exit 1; }
    if [ "$ADAPTER" = "govdata" ]; then
      # The check imports PyYAML, which the runner's system python lacks; uv supplies it for this run.
      PYTHONPATH=pgwire-calcite/src uv run --no-project --with pyyaml python pgwire-calcite/scripts/check_catalog_cache_coverage.py \
        "$expected" govdata/src/main/resources/*/*-schema.yaml
    fi
    cp "$expected" "$bundle/model/$(basename "$expected")"
  fi

  # The launcher. Nothing is written under the install tree while the server runs (it may be
  # read-only): the classpath is the jars directory, and what the server writes goes to the
  # state directory. --database defaults the reported catalog name to the adapter's; the
  # caller's arguments come after the flags and win.
  cat > "$bundle/bin/pgwire-$ADAPTER" <<SH
#!/usr/bin/env bash
HERE="\$(cd "\$(dirname "\${BASH_SOURCE[0]}")/.." && pwd)"
export PGWIRE_CALCITE_CLASSPATH_DIR="\$HERE/jars"
export PGWIRE_CALCITE_STATE_DIR="\${PGWIRE_CALCITE_STATE_DIR:-\${XDG_STATE_HOME:-\$HOME/.local/state}/pgwire-$ADAPTER}"
export PGWIRE_CALCITE_JAVA_HOME="\$HERE/jre"
export EMBED_MODEL_CACHE_DIR="\$HERE/model/embedder"
exec "\$HERE/jre/bin/java" \\
  "-Dpgwire.python=\$HERE/cpython/bin/python" \\
  -jar "\$HERE/pgwire-launcher.jar" \\
  --backend calcite --model "\$HERE/model/model.json" \\
  --database $ADAPTER $FLAGS "\$@"
SH
  chmod +x "$bundle/bin/pgwire-$ADAPTER"

  (cd "$OUT" && tar -czf "$name.tar.gz" "$name")
  # `command -v` only for its status, so its own output cannot reach the hash file
  (cd "$OUT" && (command -v sha256sum >/dev/null 2>&1 && sha256sum "$name.tar.gz" || shasum -a 256 "$name.tar.gz") \
    | awk '{print $1}' > "$name.tar.gz.sha256")
  echo "built $OUT/$name.tar.gz ($(cat "$OUT/$name.tar.gz.sha256"))"
  echo "BUNDLE_NAME=$name"
}

build_jars "$OUT/.jars-$ADAPTER"
stage_bundle "$OUT/.jars-$ADAPTER"
rm -rf "$OUT/.jars-$ADAPTER"
