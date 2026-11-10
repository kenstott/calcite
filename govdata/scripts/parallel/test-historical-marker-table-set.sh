#!/bin/bash
# #1148: a historical-complete marker must go stale when the schema's table set for that slot changes.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
fail=0; T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
ok()  { echo "ok   $1"; }
bad() { echo "FAIL $1"; fail=1; }

# fake source tree: $T/govdata/scripts/parallel + $T/govdata/src/main/resources/zz/zz-schema.yaml
SCRIPT_DIR="$T/govdata/scripts/parallel"; mkdir -p "$SCRIPT_DIR" "$T/govdata/src/main/resources/zz"
YAML="$T/govdata/src/main/resources/zz/zz-schema.yaml"
export SCRIPT_DIR GOVDATA_HISTORICAL_COMPLETE_DIR="$T/state"
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
queue=(); hcy_enqueue zz once;  [ "${#queue[@]}" = 1 ] && ok "no marker: once slot queued" || bad "no marker queued"
hcy_mark_complete zz once; hcy_mark_complete zz 2020
queue=(); hcy_enqueue zz once; hcy_enqueue zz 2020
[ "${#queue[@]}" = 0 ] && ok "marker present, table set unchanged: skipped" || bad "unchanged skipped"

# new once table added: once slot reruns, year slot does not
cat >> "$YAML" <<'Y'
  - name: snap_b
Y
queue=(); hcy_enqueue zz once; hcy_enqueue zz 2020
[ "${queue[*]}" = "zz:once" ] && ok "new once table: once slot reruns, year slot skipped" || bad "new once table (${queue[*]})"

# a table gains a year dimension: year slot reruns
cat >> "$YAML" <<'Y'
  - name: yr_b
    dimensions:
      year: {}
Y
queue=(); hcy_enqueue zz 2020
[ "${queue[*]}" = "zz:2020" ] && ok "new year table: year slot reruns" || bad "new year table (${queue[*]})"

# re-marking replaces the stale line instead of stacking
hcy_mark_complete zz once
[ "$(grep -c '^once ' "$T/state/zz.done")" = 1 ] && ok "re-mark leaves one once line" || bad "re-mark stacked"
queue=(); hcy_enqueue zz once; [ "${#queue[@]}" = 0 ] && ok "re-marked once slot skipped" || bad "re-marked"

# legacy bare marker stays valid
printf 'once\n2019\n' > "$T/state/leg.done"; mkdir -p "$T/govdata/src/main/resources/leg"; cp "$YAML" "$T/govdata/src/main/resources/leg/leg-schema.yaml"
queue=(); hcy_enqueue leg once; hcy_enqueue leg 2019; [ "${#queue[@]}" = 0 ] && ok "legacy bare markers stay valid" || bad "legacy"

exit $fail
