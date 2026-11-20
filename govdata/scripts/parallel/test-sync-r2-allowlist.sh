#!/bin/bash
# Guards the R2 sync's Iceberg allowlist (govdata-ops#1355): only folders whose metadata/ holds a
# *.metadata.json are synced, schema ref is synced in full, a failed probe fails closed, and the
# explicit override still applies. Runs the helpers out of sync-to-r2.sh against a stub rclone.
set -u
S="$(cd "$(dirname "$0")" && pwd)/sync-to-r2.sh"; fail=0
ok() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi; }

T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
R="$T/root"
mkdir -p "$R/econ/gdp/metadata" "$R/econ/nohint/metadata" "$R/econ/legacy/acc1" "$R/econ/legacy/acc2" \
         "$R/econ/emptymeta/metadata" "$R/econ/type=qcew_bulk/year=2020" "$R/ref/vectorized_chunk_codes" \
         "$R/ref/calendar/metadata" "$R/sec/year=2020" "$R/sec/filing/metadata" \
         "$R/econ/vectorized_chunk_codes" "$R/telemetry/metadata"
: > "$R/econ/gdp/metadata/v3.metadata.json"; echo 3 > "$R/econ/gdp/metadata/version-hint.text"
: > "$R/econ/nohint/metadata/v1.metadata.json"
: > "$R/econ/emptymeta/metadata/snap-1.avro"
: > "$R/sec/filing/metadata/v1.metadata.json"
: > "$R/econ/stray.marker"; : > "$R/telemetry/events.parquet"; : > "$R/telemetry/metadata/x.txt"
mkdir -p "$T/bin"
# stub rclone: lsf on a directory prints its entries (dirs with a trailing /); exit 3 when it is missing;
# STUB_FAIL=<substring> makes any path containing it fail with rc 1; every path is logged.
cat > "$T/bin/rclone" <<'STUB'
#!/bin/bash
for a in "$@"; do p="$a"; done
if [ "$1" = cat ]; then d="$STUB_ROOT/${p#*:*/}"; echo "$d" >> "$STUB_LOG"; [ -n "${STUB_FAIL:-}" ] && [[ "$d" == *"$STUB_FAIL"* ]] && exit 1; [ -f "$d" ] && cat "$d"; exit 0; fi
d="$STUB_ROOT/${p#*:*/}"; d="${d%/}"
echo "$d" >> "$STUB_LOG"
[ -n "${STUB_FAIL:-}" ] && [[ "$d" == *"$STUB_FAIL"* ]] && exit 1
if [ -f "$d" ]; then basename "$d"; exit 0; fi
[ -d "$d" ] || exit 3
for e in "$d"/*; do [ -e "$e" ] || continue; if [ -d "$e" ]; then echo "$(basename "$e")/"; else basename "$e"; fi; done
STUB
chmod +x "$T/bin/rclone"
export PATH="$T/bin:$PATH" STUB_ROOT="$R" STUB_LOG="$T/calls"; : > "$T/calls"

MINIO_REMOTE=minio; BUCKETS=(b)
sed -n '/^FULL_SCHEMAS=/,/^# ── Pointer pinning/p' "$S" | grep -v "^# ── Pointer" > "$T/helpers.sh"
log_error() { echo "$*" >&2; }
log_info() { echo "$*" >> "$T/info"; }
source "$T/helpers.sh"
STATE_DIR="$T/state"; mkdir -p "$STATE_DIR"; DRY_RUN=false; PROBE_TTL=0   # cache off until the cache section

_classify_schema econ 2>"$T/err"; rc=$?
ok "classification succeeds"                           '[ "$rc" -eq 0 ]'
ok "iceberg folder with a version-hint is kept"        '! _is_excluded econ gdp'
ok "iceberg folder without a version-hint is kept"     '! _is_excluded econ nohint'
ok "legacy folder with many children is skipped"       '_is_excluded econ legacy'
ok "metadata/ without a metadata.json is skipped"      '_is_excluded econ emptymeta'
ok "type=*_bulk plain folder is skipped"               '_is_excluded econ type=qcew_bulk'
ok "skipped names are logged once"                     '[ "$(grep -c "skipping non-Iceberg folders" "$T/info")" -eq 1 ] && grep -q "legacy" "$T/info" && grep -q "type=qcew_bulk" "$T/info"'
ok "exclude flags carry the skipped folders"           '_set_exclude_args econ; [[ "${_EXARGS[*]}" == *"--exclude /legacy/**"* && "${_EXARGS[*]}" != *"/gdp/**"* ]]'
ok "no recursive listing of a skipped folder"          '! grep -qE "/legacy/acc|/legacy$|/type=qcew_bulk/year" "$T/calls"'
ok "plain vectorized_chunk_codes is synced anyway"     '! _is_excluded econ vectorized_chunk_codes'
ok "vectorized_chunk_codes is not probed"              '! grep -q "/econ/vectorized_chunk_codes/metadata" "$T/calls"'
ok "a plain root file is skipped"                      '_is_excluded econ stray.marker'
ok "root file exclude flag matches the file itself"    '_set_exclude_args econ; [[ "${_EXARGS[*]}" == *"--exclude /stray.marker "* ]]'
ok "exception list is logged"                          'grep -q "plain exceptions.*vectorized_chunk_codes" "$T/info"'
ok "the skip set does not leak to another schema"      '! _is_excluded sec legacy'

: > "$T/info"; _classify_schema ref 2>/dev/null; rc=$?
ok "ref is synced in full, nothing skipped"            '[ "$rc" -eq 0 ] && ! _is_excluded ref vectorized_chunk_codes && ! _is_excluded ref calendar'
ok "ref is not probed"                                 '! grep -q "/ref/vectorized_chunk_codes/metadata" "$T/calls"'

_classify_schema telemetry 2>/dev/null
ok "telemetry parquet files are synced"                '! _is_excluded telemetry events.parquet'
ok "telemetry folder is synced"                        '! _is_excluded telemetry metadata'
GOVDATA_R2_SYNC_ALWAYS="" ; ALWAYS_SYNC=""; _classify_schema econ 2>/dev/null
ok "exceptions can be overridden away"                 '_is_excluded econ vectorized_chunk_codes'
ALWAYS_SYNC="*/vectorized_chunk_codes telemetry/*"

STUB_FAIL="/econ/nohint/metadata" _classify_schema econ 2>"$T/err"; rc=$?
ok "a failing probe fails the classification"          '[ "$rc" -eq 75 ]'
ok "a hinted table is decided by one lookup of the hint" '! grep -q "/econ/gdp/metadata$" "$T/calls" && grep -q "/econ/gdp/metadata/version-hint.text" "$T/calls"'
ok "a failed probe is logged"                          'grep -q "Iceberg probe failed" "$T/err"'
ok "a failed probe skips nothing"                      '! _is_excluded econ legacy'
STUB_FAIL="/sec" _classify_schema sec 2>/dev/null; rc=$?
ok "a failing directory listing fails the classification" '[ "$rc" -eq 75 ]'

_classify_schema sec 2>/dev/null
ok "denylist still excludes sec/year=*"                '_is_excluded sec year=2020'
ok "the denylisted folder is not probed"               '! grep -q "/sec/year=2020/metadata" "$T/calls"'
LEGACY_EXCLUDE='econ/gdp' _classify_schema econ 2>/dev/null
ok "override excludes an Iceberg table"                'LEGACY_EXCLUDE="econ/gdp"; _is_excluded econ gdp'
ok "override covers a table in a full-sync schema"     'LEGACY_EXCLUDE="ref/calendar"; _classify_schema ref 2>/dev/null; _is_excluded ref calendar'

# probe cache: hit, expiry, failure not cached
PROBE_TTL=3600; rm -f "$STATE_DIR"/.iceberg-*
: > "$T/calls"; _probe_iceberg econ gdp; _probe_iceberg econ legacy; rc1=$?
n1=$(wc -l < "$T/calls")
_probe_iceberg econ gdp; a=$?; _probe_iceberg econ legacy; b=$?
ok "a cached probe is not repeated"                    '[ "$(wc -l < "$T/calls")" -eq "$n1" ] && [ "$a" -eq 0 ] && [ "$b" -eq 1 ]'
touch -d '2 hours ago' "$STATE_DIR/.iceberg-econ-gdp"
_probe_iceberg econ gdp
ok "an expired entry is probed again"                  '[ "$(wc -l < "$T/calls")" -eq $((n1 + 1)) ]'
STUB_FAIL="/econ/nohint/metadata" _probe_iceberg econ nohint 2>/dev/null; rc=$?
ok "a failed probe is rc 75 and is not cached"         '[ "$rc" -eq 75 ] && [ ! -e "$STATE_DIR/.iceberg-econ-nohint" ]'
_probe_iceberg econ nohint
ok "the next pass probes the folder again and caches"  '[ "$(cat "$STATE_DIR/.iceberg-econ-nohint")" = yes ]'
exit $fail
