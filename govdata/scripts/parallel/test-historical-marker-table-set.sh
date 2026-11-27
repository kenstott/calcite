#!/bin/bash
# #1148: a historical-complete marker must go stale when the schema's table set for that slot changes.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
fail=0; T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
ok()  { echo "ok   $1"; }
bad() { echo "FAIL $1"; fail=1; }

# fake source tree ($T/govdata/...) and a fake jar built from $T/jar-root. They are separate so the
# tree can run ahead of the jar.
SCRIPT_DIR="$T/govdata/scripts/parallel"; mkdir -p "$SCRIPT_DIR" "$T/govdata/src/main/resources/zz" "$T/jar-root/zz"
TREE_YAML="$T/govdata/src/main/resources/zz/zz-schema.yaml"
YAML="$T/jar-root/zz/zz-schema.yaml"
export SCRIPT_DIR GOVDATA_HISTORICAL_COMPLETE_DIR="$T/state" GOVDATA_JAR="$T/fake.jar"
build_jar() { rm -f "$GOVDATA_JAR"; (cd "$T/jar-root" && python3 -m zipfile -c "$GOVDATA_JAR" ./*); }
log_info()  { :; }
log_error() { echo "ERR $*" >&2; }
schema_yaml_resource() { echo "$1/$1-schema.yaml"; }
source "$HERE/historical-year-complete.sh"

cat > "$YAML" <<'Y'
tables:
  - name: snap_a
  - name: yr_a
    dimensions:
      year: {}
Y
cp "$YAML" "$TREE_YAML"; build_jar
queue=(); hcy_enqueue zz once;  [ "${#queue[@]}" = 1 ] && ok "no marker: once slot queued" || bad "no marker queued"
hcy_mark_complete zz once; hcy_mark_complete zz 2020
queue=(); hcy_enqueue zz once; hcy_enqueue zz 2020
[ "${#queue[@]}" = 0 ] && ok "marker present, table set unchanged: skipped" || bad "unchanged skipped"

# new once table added: once slot reruns, year slot does not
cat >> "$YAML" <<'Y'
  - name: snap_b
Y
build_jar
queue=(); hcy_enqueue zz once; hcy_enqueue zz 2020
[ "${queue[*]}" = "zz:once" ] && ok "new once table: once slot reruns, year slot skipped" || bad "new once table (${queue[*]})"

# a table gains a year dimension: year slot reruns
cat >> "$YAML" <<'Y'
  - name: yr_b
    dimensions:
      year: {}
Y
build_jar
queue=(); hcy_enqueue zz 2020
[ "${queue[*]}" = "zz:2020" ] && ok "new year table: year slot reruns" || bad "new year table (${queue[*]})"

# re-marking replaces the stale line instead of stacking
hcy_mark_complete zz once
[ "$(grep -c '^once ' "$T/state/zz.done")" = 1 ] && ok "re-mark leaves one once line" || bad "re-mark stacked"
queue=(); hcy_enqueue zz once; [ "${#queue[@]}" = 0 ] && ok "re-marked once slot skipped" || bad "re-marked"

# #1588: a table in the source tree but not in the jar the pool runs is not claimed by the marker;
# the slot runs again once the pool's jar has it
hcy_mark_complete zz 2020
cat "$YAML" > "$TREE_YAML"; printf '  - name: yr_c\n    dimensions:\n      year: {}\n' >> "$TREE_YAML"
queue=(); hcy_enqueue zz 2020; [ "${#queue[@]}" = 0 ] && ok "tree ahead of jar: nothing new to run" || bad "tree ahead queued"
hcy_mark_complete zz 2020
cp "$TREE_YAML" "$YAML"; build_jar
queue=(); hcy_enqueue zz 2020
[ "${queue[*]}" = "zz:2020" ] && ok "rebuilt jar with the new table: year slot reruns" || bad "marker claimed a table the run never loaded"

# no jar, no marker
(GOVDATA_JAR="$T/missing.jar"; hcy_mark_complete zz 2031) 2>/dev/null && bad "marked without a jar" || ok "no jar: marking fails"
grep -q '^2031' "$T/state/zz.done" && bad "marker line written without a jar" || ok "no jar: no marker line"

# legacy bare marker stays valid
printf 'once\n2019\n' > "$T/state/leg.done"; mkdir -p "$T/jar-root/leg"; cp "$YAML" "$T/jar-root/leg/leg-schema.yaml"; build_jar
queue=(); hcy_enqueue leg once; hcy_enqueue leg 2019; [ "${#queue[@]}" = 0 ] && ok "legacy bare markers stay valid" || bad "legacy"

# #1349: every sec pool slot resolves to a bundled schema YAML that exists
eval "$(sed -n '/^schema_yaml_resource()/,/^}/p' "$HERE/common.sh")"
for slot in sec_primary sec_secondary sec_13f sec_prices; do
  [ -f "$HERE/../../src/main/resources/$(schema_yaml_resource "$slot")" ] && ok "$slot resolves to an existing YAML" || bad "$slot YAML missing"
done
SCRIPT_DIR="$HERE"
(cd "$HERE/../../src/main/resources" && python3 -m zipfile -c "$T/sec.jar" sec); GOVDATA_JAR="$T/sec.jar"
fp=$(hcy_fingerprint sec_13f 2025) && [ -n "$fp" ] && ok "sec_13f:2025 fingerprints" || bad "sec_13f fingerprint"

exit $fail
