#!/bin/bash
# Integration test: force-reprocess.sh refuses a second run of the same schema+years (exit 3, nothing launched)
# and takes no claim on --dry-run. Reads the tracker (read-only); the refusal path and --dry-run launch no pool.
set -u
D="$(cd "$(dirname "$0")" && pwd)"; T=$(mktemp -d); trap 'kill $(jobs -p) 2>/dev/null; rm -rf "$T"' EXIT
export GOVDATA_CLAIM_DIR="$T/claims" GOVDATA_PARQUET_DIR="s3://govdata-parquet-v1" GOVDATA_TMPDIR="$T/tmp"
mkdir -p "$GOVDATA_CLAIM_DIR"
fail=0; ck() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi; }
# a live owner that looks like force-reprocess.sh, holding ag 2010-2026 on the production bucket
bash -c 'exec -a force-reprocess.sh sleep 100' >/dev/null 2>&1 & OWNER=$!; sleep 0.3
printf "c_pid=$OWNER\nc_bucket='s3://govdata-parquet-v1'\nc_schema='ag'\nc_start=2010\nc_end=2026\nc_tables='nass_land_values'\nc_since=$(date +%%s)\n" > "$GOVDATA_CLAIM_DIR/$OWNER.claim"

out=$(timeout 90 bash "$D/force-reprocess.sh" --schema ag --tables nass_land_values --start 2010 --end 2026 2>&1); rc=$?
ck "a second run of the same schema+years exits 3"                           '[ $rc -eq 3 ]'
ck "...naming the run that holds it"                                        'echo "$out" | grep -q "force-reprocess.sh pid $OWNER"'
ck "...before it launched a pool (no Historical/Daily banner)"               '! echo "$out" | grep -q -E "^── (Historical|Daily):"'
ck "...and took no claim of its own"                                        '[ "$(ls "$GOVDATA_CLAIM_DIR"/*.claim | wc -l)" -eq 1 ]'

out=$(timeout 90 bash "$D/force-reprocess.sh" --schema ag --tables nass_land_values --start 2010 --end 2026 --dry-run 2>&1); rc=$?
ck "--dry-run is not refused (it starts nothing)"                            '[ $rc -eq 0 ] && echo "$out" | grep -q "DRY RUN"'
ck "--dry-run takes no claim"                                               '[ "$(ls "$GOVDATA_CLAIM_DIR"/*.claim | wc -l)" -eq 1 ]'

# a DQ-bucket run of the same schema+years must pass the claim check (dry-run keeps it harmless)
out=$(GOVDATA_PARQUET_DIR="s3://govdata-parquet-v1-dq" timeout 90 bash "$D/force-reprocess.sh" --schema ag --tables nass_land_values --start 2010 --end 2026 --dry-run 2>&1); rc=$?
ck "a DQ-bucket run of the same schema+years is not refused"                 '[ $rc -ne 3 ]'
exit $fail
