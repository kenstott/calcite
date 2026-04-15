#!/usr/bin/env bash
# r2-ops-report.sh — Query Cloudflare GraphQL Analytics for R2 operation counts
#
# Usage:
#   ./r2-ops-report.sh [--days N] [--bucket BUCKET_NAME] [--start YYYY-MM-DD] [--end YYYY-MM-DD]
#
# Defaults: last 7 days, all buckets
#
# Requires: curl, jq
# Credentials sourced from .env.prod (CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../.env.prod"

# ---------------------------------------------------------------------------
# Load credentials
# ---------------------------------------------------------------------------
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  set -a; source "$ENV_FILE"; set +a
fi

: "${CLOUDFLARE_ACCOUNT_ID:?CLOUDFLARE_ACCOUNT_ID not set (add to .env.prod)}"
: "${CLOUDFLARE_API_TOKEN:?CLOUDFLARE_API_TOKEN not set (add to .env.prod)}"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
DAYS=7
BUCKET=""
START_DATE=""
END_DATE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --days)   DAYS="$2";       shift 2 ;;
    --bucket) BUCKET="$2";     shift 2 ;;
    --start)  START_DATE="$2"; shift 2 ;;
    --end)    END_DATE="$2";   shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

# Compute date range
if [[ -z "$START_DATE" ]]; then
  START_DATE=$(date -u -v-"${DAYS}"d +%Y-%m-%d 2>/dev/null \
    || date -u --date="${DAYS} days ago" +%Y-%m-%d)
fi
if [[ -z "$END_DATE" ]]; then
  END_DATE=$(date -u +%Y-%m-%d)
fi

echo "R2 Operations Report"
echo "Account : $CLOUDFLARE_ACCOUNT_ID"
echo "Range   : $START_DATE → $END_DATE"
[[ -n "$BUCKET" ]] && echo "Bucket  : $BUCKET"
echo "-----------------------------------------------------------"

# ---------------------------------------------------------------------------
# Build GraphQL query — jq --arg handles all quoting/escaping automatically
# ---------------------------------------------------------------------------
if [[ -n "$BUCKET" ]]; then
  BUCKET_FILTER_GQL=", bucketName: \"$BUCKET\""
else
  BUCKET_FILTER_GQL=""
fi

GQL="{ viewer { accounts(filter: { accountTag: \"$CLOUDFLARE_ACCOUNT_ID\" }) { r2OperationsAdaptiveGroups( limit: 1000, filter: { date_geq: \"$START_DATE\", date_leq: \"$END_DATE\"$BUCKET_FILTER_GQL }, orderBy: [date_ASC, actionType_ASC] ) { sum { requests } dimensions { actionType date bucketName } } } } }"

QUERY=$(jq -n --arg q "$GQL" '{"query": $q}')

# ---------------------------------------------------------------------------
# Execute request — write response to temp file to avoid subshell scope issues
# ---------------------------------------------------------------------------
TMPFILE=$(mktemp /tmp/r2-ops-XXXXXX.json)
trap 'rm -f "$TMPFILE"' EXIT

curl -s -X POST \
  "https://api.cloudflare.com/client/v4/graphql" \
  -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
  -H "Content-Type: application/json" \
  --data "$QUERY" > "$TMPFILE"

# Check for errors
ERRORS=$(jq -r 'if (.errors | (. != null and length > 0)) then .errors | tostring else empty end' "$TMPFILE")
if [[ -n "$ERRORS" ]]; then
  echo "GraphQL errors:" >&2
  echo "$ERRORS" >&2
  exit 1
fi

# Write just the groups array to a second temp file
GROUPS_FILE=$(mktemp /tmp/r2-groups-XXXXXX.json)
trap 'rm -f "$TMPFILE" "$GROUPS_FILE"' EXIT

jq '.data.viewer.accounts[0].r2OperationsAdaptiveGroups // []' "$TMPFILE" > "$GROUPS_FILE"

COUNT=$(jq 'length' "$GROUPS_FILE")
if [[ "$COUNT" -eq 0 ]]; then
  echo "No data returned for this range."
  exit 0
fi

CLASS_A_OPS="ListObjects|PutObject|CopyObject|DeleteObject|DeleteObjects|CreateMultipartUpload|UploadPart|UploadPartCopy|CompleteMultipartUpload|AbortMultipartUpload|ListMultipartUploads|ListParts"

# ---------------------------------------------------------------------------
# Summary by actionType (across all dates)
# ---------------------------------------------------------------------------
echo ""
echo "=== By Action Type (total) ==="
printf "%-30s %15s\n" "ACTION" "REQUESTS"
printf "%-30s %15s\n" "------------------------------" "---------------"

jq -r --arg ops "$CLASS_A_OPS" '
  group_by(.dimensions.actionType)[]
  | { action: .[0].dimensions.actionType, total: (map(.sum.requests) | add) }
  | [.action, (.total | tostring)]
  | @tsv' "$GROUPS_FILE" \
| sort -t$'\t' -k2 -rn \
| awk -F'\t' '{ printf "%-30s %15s\n", $1, $2 }'

# ---------------------------------------------------------------------------
# Class A vs Class B breakdown
# ---------------------------------------------------------------------------
echo ""
echo "=== Class A vs Class B ==="

CLASS_A=$(jq -r --arg ops "$CLASS_A_OPS" '
  map(select(.dimensions.actionType | test($ops)))
  | map(.sum.requests) | add // 0' "$GROUPS_FILE")
CLASS_B=$(jq -r --arg ops "$CLASS_A_OPS" '
  map(select(.dimensions.actionType | test($ops) | not))
  | map(.sum.requests) | add // 0' "$GROUPS_FILE")
TOTAL=$(jq '[.[].sum.requests] | add // 0' "$GROUPS_FILE")

CLASS_A_COST=$(echo "scale=4; $CLASS_A * 4.50 / 1000000" | bc 2>/dev/null || echo "n/a")
CLASS_B_COST=$(echo "scale=4; $CLASS_B * 0.36 / 1000000" | bc 2>/dev/null || echo "n/a")

printf "%-30s %15s  %10s\n" "CLASS" "REQUESTS" "EST. COST"
printf "%-30s %15s  %10s\n" "------------------------------" "---------------" "----------"
printf "%-30s %15s  %10s\n" "Class A (list/create/copy)" "$CLASS_A" "\$$CLASS_A_COST"
printf "%-30s %15s  %10s\n" "Class B (get/head/etc)"     "$CLASS_B" "\$$CLASS_B_COST"
printf "%-30s %15s\n"       "Total"                      "$TOTAL"

# ---------------------------------------------------------------------------
# Daily breakdown
# ---------------------------------------------------------------------------
echo ""
echo "=== Daily Totals ==="
printf "%-12s %15s %15s %15s\n" "DATE" "CLASS A" "CLASS B" "TOTAL"
printf "%-12s %15s %15s %15s\n" "------------" "---------------" "---------------" "---------------"

jq -r --arg ops "$CLASS_A_OPS" '
  group_by(.dimensions.date)[]
  | {
      date: .[0].dimensions.date,
      classA: (map(select(.dimensions.actionType | test($ops))) | map(.sum.requests) | add // 0),
      classB: (map(select(.dimensions.actionType | test($ops) | not)) | map(.sum.requests) | add // 0)
    }
  | [.date, (.classA | tostring), (.classB | tostring), ((.classA + .classB) | tostring)]
  | @tsv' "$GROUPS_FILE" \
| awk -F'\t' '{ printf "%-12s %15s %15s %15s\n", $1, $2, $3, $4 }'

# ---------------------------------------------------------------------------
# Per-bucket breakdown (if multiple buckets present)
# ---------------------------------------------------------------------------
BUCKET_COUNT=$(jq '[.[].dimensions.bucketName] | unique | length' "$GROUPS_FILE")
if [[ "$BUCKET_COUNT" -gt 1 ]]; then
  echo ""
  echo "=== By Bucket ==="
  printf "%-30s %15s %15s\n" "BUCKET" "CLASS A" "CLASS B"
  printf "%-30s %15s %15s\n" "------------------------------" "---------------" "---------------"
  jq -r --arg ops "$CLASS_A_OPS" '
    group_by(.dimensions.bucketName)[]
    | {
        bucket: .[0].dimensions.bucketName,
        classA: (map(select(.dimensions.actionType | test($ops))) | map(.sum.requests) | add // 0),
        classB: (map(select(.dimensions.actionType | test($ops) | not)) | map(.sum.requests) | add // 0)
      }
    | [.bucket, (.classA | tostring), (.classB | tostring)]
    | @tsv' "$GROUPS_FILE" \
  | awk -F'\t' '{ printf "%-30s %15s %15s\n", $1, $2, $3 }'
fi

echo ""
echo "Note: Free tier: 1M Class A ops/month, 10M Class B ops/month."
echo "      Costs shown are estimates for ops above the free tier."
