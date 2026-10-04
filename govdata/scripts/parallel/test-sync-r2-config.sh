#!/bin/bash
# Guards the R2 sync's rclone timeout and checker settings (see the comments in sync-to-r2.sh).
set -u
S="$(cd "$(dirname "$0")" && pwd)/sync-to-r2.sh"; fail=0
ok() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi; }
ok "RCLONE_TIMEOUT is exported with a default above rclone's 5m"  'grep -q "^export RCLONE_TIMEOUT=\"\${GOVDATA_R2_SYNC_TIMEOUT:-30m}\"" "$S"'
ok "RCLONE_CONTIMEOUT is exported"                                'grep -q "^export RCLONE_CONTIMEOUT=" "$S"'
ok "checkers come from CHECKERS, default 4"                       'grep -q "^CHECKERS=\"\${GOVDATA_R2_SYNC_CHECKERS:-4}\"" "$S" && grep -q -- "--checkers \$CHECKERS" "$S"'
ok "transfers default 8, from TRANSFERS"                          'grep -q "^TRANSFERS=\"\${GOVDATA_R2_SYNC_TRANSFERS:-8}\"" "$S" && grep -q -- "--transfers \$TRANSFERS" "$S" && ! grep -q -- "--transfers 16" "$S"'
ok "tpslimit 25 / burst 10, from variables"                       'grep -q "^TPSLIMIT=\"\${GOVDATA_R2_SYNC_TPSLIMIT:-25}\"" "$S" && grep -q "^TPSLIMIT_BURST=\"\${GOVDATA_R2_SYNC_TPSLIMIT_BURST:-10}\"" "$S" && grep -q -- "--tpslimit \$TPSLIMIT --tpslimit-burst \$TPSLIMIT_BURST" "$S"'
ok "no hard-coded 32 checkers remain"                             '! grep -q -- "--checkers 32" "$S"'
# the exports must come before the first rclone call, or they would not cover it
first=$(grep -n "rclone " "$S" | grep -v "^[0-9]*:#" | head -1 | cut -d: -f1); exp=$(grep -n "^export RCLONE_TIMEOUT" "$S" | head -1 | cut -d: -f1)
ok "timeouts are exported before the first rclone call"           '[ -n "$exp" ] && [ "$exp" -lt "$first" ]'
ok "a per-slice cap wraps the copy (default 60m)"                 'grep -q "^SLICE_MAX=\"\${GOVDATA_R2_SYNC_SLICE_MAX:-60m}\"" "$S" && grep -q "timeout --kill-after=60 \"\$SLICE_MAX\" rclone copy" "$S"'
ok "large schemas are split per top-level directory (default: sec)" 'grep -q "^SPLIT_SCHEMAS=\"\${GOVDATA_R2_SYNC_SPLIT_SCHEMAS:-sec}\"" "$S" && grep -q "^UNIT_MAX=\"\${GOVDATA_R2_SYNC_UNIT_MAX:-20m}\"" "$S" && grep -q "_copy_split_schema \"\$s\"" "$S"'
exit $fail
