#!/usr/bin/env bash
#
# fetch-submissions-bulk.sh <out_dir>
#
# Downloads SEC's nightly submissions.zip (about 1.6 GB, one JSON per filer) to
# out_dir/submissions.zip: the file sec.bulkSubmissionsZip (SEC_BULK_SUBMISSIONS_ZIP) points a worker
# at, so the per-filer data.sec.gov/submissions/CIK*.json fetches are answered locally. It is read in
# place, never unpacked.
#
# One request. A conditional GET (ETag kept in out_dir/submissions.etag) skips the download when SEC's
# copy has not changed since the last fetch. The zip is regenerated nightly (about 00:15 ET), so
# filings made after that are not in it; the ETL rechecks any miss live.
#
# Transient: delete out_dir when the run that used it is over.
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <out_dir>" >&2
  exit 1
fi
OUT_DIR="$1"
UA="${SEC_USER_AGENT:-Apache Calcite SEC Adapter apache-calcite@apache.org}"
URL="https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
mkdir -p "$OUT_DIR"
ZIP="$OUT_DIR/submissions.zip"
ETAG="$OUT_DIR/submissions.etag"
TMP="$ZIP.part"

echo "[fetch-submissions-bulk] $URL"
code=$(curl -sS -L -A "$UA" -o "$TMP" -w '%{http_code}' \
  --etag-save "$ETAG.new" ${ETAG:+$( [ -f "$ZIP" ] && [ -f "$ETAG" ] && echo "--etag-compare $ETAG" )} "$URL")
case "$code" in
  200) mv -f "$TMP" "$ZIP"; mv -f "$ETAG.new" "$ETAG"
       echo "[fetch-submissions-bulk] wrote $ZIP ($(du -h "$ZIP" | cut -f1))" ;;
  304) rm -f "$TMP" "$ETAG.new"
       echo "[fetch-submissions-bulk] unchanged since the last fetch: $ZIP" ;;
  *)   rm -f "$TMP" "$ETAG.new"
       echo "[fetch-submissions-bulk] unexpected HTTP $code" >&2; exit 1 ;;
esac
