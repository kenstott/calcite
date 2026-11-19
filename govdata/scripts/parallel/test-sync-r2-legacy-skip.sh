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
ok "vectorized_chunk_codes is not excluded"        '! _is_excluded sec vectorized_chunk_codes'
ok "type=*_bulk is not excluded by default"        '! _is_excluded econ type=qcew_bulk'
ok "vectorized_chunk_codes has no exclude flag"    '_set_exclude_args sec; [[ "${_EXARGS[*]}" != *vectorized_chunk_codes* ]]'
ok "a new file in vectorized_chunk_codes is listed" 'mkdir -p "$T/sec/vectorized_chunk_codes" && : > "$T/sec/vectorized_chunk_codes/new.parquet" && _lsf --files-only "minio:b/sec/vectorized_chunk_codes" | grep -q new.parquet'
ok "exclude flags for sec name year=*"             '_set_exclude_args sec; [ "${_EXARGS[*]}" = "--exclude /year=*/**" ]'
ok "exclude flags for another schema omit year=*"  '_set_exclude_args econ; [ "${#_EXARGS[@]}" -eq 0 ]'
ok "no listing inside an excluded folder"          'grep -q "/year=2020\$" "$STUB_LOG" && false || true'

start=$(date +%s)
LIST_MAX=2s STUB_HANG=1 _lsf --dirs-only "minio:b/sec" >/dev/null 2>"$T/err"; rc=$?
el=$(( $(date +%s) - start ))
ok "a hung listing is cut at its deadline"         '[ "$rc" -ne 0 ] && [ "$el" -lt 10 ]'
ok "an expired listing is logged"                  'grep -q "listing failed or exceeded" "$T/err"'
# An override pattern without a leading slash is never expanded against the host filesystem.
mkdir -p "$T/cwd/tmpfoo" "$T/cwd/other"; ( cd "$T/cwd" &&
  LEGACY_EXCLUDE='sec/year=* */tmp*' && _set_exclude_args sec && [ "${_EXARGS[*]}" = "--exclude /year=*/** --exclude /tmp*/**" ] && \
  _is_excluded sec tmpfoo ) && echo "ok   override globs are not pathname-expanded" || { echo "FAIL override globs are not pathname-expanded"; fail=1; }
exit $fail
