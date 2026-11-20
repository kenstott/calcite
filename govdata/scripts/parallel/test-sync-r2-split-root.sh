#!/bin/bash
# Guards the split path of the R2 sync (govdata-ops#1355): the schema-root unit must carry the exclude flag for a
# skipped plain root file, so a file that is logged as skipped is not copied. Runs the real sync-to-r2.sh against
# a stub rclone (no network, throwaway $HOME).
set -u
SCRIPT="${1:-$(cd "$(dirname "$0")" && pwd)/sync-to-r2.sh}"
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin" "$T/home"
cat > "$T/bin/rclone" <<'STUB'
#!/bin/bash
case "$1" in
  lsf)
    if [[ " $* " == *" -R "* ]]; then echo "2026-10-01 00:00:00"; exit 0; fi
    last="${@: -1}"
    if [[ "$last" == */metadata/ ]]; then echo v1.metadata.json; exit 0; fi
    if [[ " $* " == *" --files-only "* ]]; then echo stray.marker; exit 0; fi
    if   [[ "$last" =~ ^[a-z0-9_]+:[A-Za-z0-9._-]+$ ]];        then printf 'bigone/\n'
    elif [[ "$last" =~ ^[a-z0-9_]+:[A-Za-z0-9._-]+/bigone$ ]]; then printf 'ok1/\n'
    fi
    exit 0 ;;
  copy) echo "$*" >> "$STUB_LOG"; exit 0 ;;
  *) exit 0 ;;
esac
STUB
chmod +x "$T/bin/rclone"
export PROD_AWS_ACCESS_KEY_ID=test PROD_AWS_SECRET_ACCESS_KEY=test PROD_AWS_ENDPOINT_OVERRIDE=http://127.0.0.1:1
export STUB_LOG="$T/copies.log" HOME="$T/home" PATH="$T/bin:$PATH"
export GOVDATA_R2_SYNC_SPLIT_SCHEMAS=bigone GOVDATA_R2_SYNC_PRIORITY_SCHEMAS=bigone
fail=0
ck() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi; }
timeout 150 bash "$SCRIPT" --schemas bigone > "$T/p.log" 2>&1
ck "the root unit copy carries the exclude for the skipped root file" 'grep -E -- "max-depth 1" "$STUB_LOG" | grep -q -- "--exclude /stray.marker"'
ck "the iceberg unit copy does not exclude it"                         '! grep "/bigone/ok1 " "$STUB_LOG" | grep -q -- "stray.marker"'
ck "the split log names the skipped root file"                         'grep -q "root unit excludes non-Iceberg entries: stray.marker" "$T/p.log"'
exit $fail
