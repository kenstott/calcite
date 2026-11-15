#!/bin/bash
# Guards the R2 sync's skip of legacy non-Iceberg folders and its listing deadlines (govdata-ops#1226).
# Sources the helper functions out of sync-to-r2.sh and runs them against a stub rclone, so no MinIO is touched.
set -u
S="$(cd "$(dirname "$0")" && pwd)/sync-to-r2.sh"; fail=0
ok() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi; }

T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin" "$T/sec/year=2020" "$T/sec/filing_metadata/metadata"
for i in $(seq 1 20000); do : > "$T/sec/year=2020/acc$i"; done
# stub rclone: "lsf --dirs-only <remote>:<bucket>/<path>" lists directories of $T/<path>; "hang" sleeps
cat > "$T/bin/rclone" <<'STUB'
#!/bin/bash
[ "${STUB_HANG:-}" = 1 ] && sleep 30
for a in "$@"; do p="$a"; done
d="$STUB_ROOT/${p#*:*/}"
echo "$d" >> "$STUB_LOG"
ls -1 "$d" 2>/dev/null | sed 's#$#/#'
STUB
chmod +x "$T/bin/rclone"
export PATH="$T/bin:$PATH" STUB_ROOT="$T" STUB_LOG="$T/calls"; : > "$T/calls"

# extract helper block: from LEGACY_EXCLUDE through the end of _lsf
sed -n '/^LEGACY_EXCLUDE=/,/^# ── Pointer pinning/p' "$S" | grep -v "^# ── Pointer" > "$T/helpers.sh"
log_error() { echo "$*" >&2; }
source "$T/helpers.sh"

ok "sec/year=2020 is excluded"                     '_is_excluded sec year=2020'
ok "sec/filing_metadata is not excluded"           '! _is_excluded sec filing_metadata'
ok "any schema's type=*_bulk is excluded"          '_is_excluded econ type=qcew_bulk'
ok "exclude flags for sec name year=*"             '[[ "$(_exclude_flags sec)" == *"--exclude /year=*/**"* ]]'
ok "exclude flags for another schema omit year=*"  '[[ "$(_exclude_flags econ)" != *"year=*"* ]]'
ok "no listing inside an excluded folder"          'grep -q "/year=2020\$" "$STUB_LOG" && false || true'

start=$(date +%s)
LIST_MAX=2s STUB_HANG=1 _lsf --dirs-only "minio:b/sec" >/dev/null 2>"$T/err"; rc=$?
el=$(( $(date +%s) - start ))
ok "a hung listing is cut at its deadline"         '[ "$rc" -ne 0 ] && [ "$el" -lt 10 ]'
ok "an expired listing is logged"                  'grep -q "listing failed or exceeded" "$T/err"'
exit $fail
