#!/usr/bin/env bash
#
# fetch-13f-bulk.sh <out_dir> <release>...
#
# Downloads SEC Form 13F data sets and turns each one's INFOTABLE into out_dir/infotable_<release>.parquet,
# sorted by ACCESSION_NUMBER with every column as text: the files sec.bulk13fDir
# (SEC_BULK_13F_DIR) points a worker at, so a 13F filing in them costs no information-table fetch.
#
# <release> is a quarter such as 2023q4, a date range such as 01mar2026-31may2026, or a full URL.
# The downloaded zip and the extracted TSV are deleted as soon as the parquet is written, and the
# parquet files are transient too: delete out_dir when the run that used them is over.
#
# One request per release. SEC asks for a descriptive User-Agent; the declared contact comes from sec-schema.yaml (edgarUserAgent) via the govdata jar (GOVDATA_JAR).
set -euo pipefail

if [ $# -lt 2 ]; then
  echo "Usage: $0 <out_dir> <release>..." >&2
  exit 1
fi
OUT_DIR="$1"; shift
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/sec-cooldown.sh"
sec_cooldown_init
sec_cooldown_refuse fetch-13f-bulk || exit "$SEC_COOLDOWN_EXIT"
sec_user_agent_init || exit 1
UA="$SEC_UA"
BASE="${F13_BASE_URL:-https://www.sec.gov/files/structureddata/data/form-13f-data-sets}"  # override only for tests against a local server
mkdir -p "$OUT_DIR"

for release in "$@"; do
  case "$release" in
    http*) url="$release"; name="$(basename "$release" _form13f.zip)" ;;
    *)     url="$BASE/${release}_form13f.zip"; name="$release" ;;
  esac
  out="$OUT_DIR/infotable_${name}.parquet"
  work="$(mktemp -d "$OUT_DIR/.work.XXXXXX")"
  trap 'rm -rf "$work"' EXIT

  echo "[fetch-13f-bulk] $name <- $url"
  sec_cooldown_refuse fetch-13f-bulk || exit "$SEC_COOLDOWN_EXIT"
  code=$(curl -sS -L -A "$UA" -D "$work/hdr" -o "$work/z.zip" -w '%{http_code}' "$url")
  sec_cooldown_on_status fetch-13f-bulk "$code" "$work/hdr" || exit "$SEC_COOLDOWN_EXIT"
  if [ "$code" != 200 ]; then
    echo "[fetch-13f-bulk] unexpected HTTP $code for $url" >&2; exit 1
  fi
  unzip -q -o "$work/z.zip" INFOTABLE.tsv -d "$work"
  rm -f "$work/z.zip"
  duckdb -c "
    COPY (SELECT * FROM read_csv('$work/INFOTABLE.tsv', delim='\t', header=true,
                                  all_varchar=true, quote='')
          ORDER BY ACCESSION_NUMBER)
    TO '$out.tmp' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);"
  mv -f "$out.tmp" "$out"
  rm -rf "$work"
  trap - EXIT
  echo "[fetch-13f-bulk] wrote $out ($(du -h "$out" | cut -f1))"
done
