#!/usr/bin/env bash
#
# fetch-edgar-feed.sh <feed_dir> <day>...      where <day> is YYYYMMDD or a range YYYYMMDD..YYYYMMDD
#
# Streams one or more days of EDGAR's daily feed
# (https://www.sec.gov/Archives/edgar/Feed/<YYYY>/QTR<N>/<YYYYMMDD>.nc.tar.gz, about 1-4 GB each) and
# unpacks the documents of the forms this adapter processes into feed_dir, the tree
# sec.feedDir (SEC_FEED_DIR) points a worker at: a document request it can answer is read locally
# instead of fetched from sec.gov.
#
# One request per day; the day file is read as a stream and never stored, so only the unpacked
# documents use disk (text documents only; images, spreadsheets and zips are skipped). The tree is
# transient: delete feed_dir when the run that used it is over. The feed publishes a day's file after
# that day closes, so today's filings are not in it and are fetched from sec.gov as usual.
#
# Disk: the unpacked tree is about 15% of the bytes downloaded (one 450 GB year is about 65 GB), so
# unpack a half year or a quarter at a time if /var/tmp/govdata is short of space.
#
# Needs the govdata shadow jar: GOVDATA_JAR, else govdata/build/libs/sih-govdata.jar.
set -euo pipefail

if [ $# -lt 2 ]; then
  echo "Usage: $0 <feed_dir> <YYYYMMDD>..." >&2
  exit 1
fi
FEED_DIR="$1"; shift
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="${GOVDATA_JAR:-$SCRIPT_DIR/../build/libs/sih-govdata.jar}"
[ -f "$JAR" ] || { echo "jar not found: $JAR (set GOVDATA_JAR)" >&2; exit 1; }

# A range expands to its weekdays; weekends and holidays have no feed file and are skipped by the
# unpacker (HTTP 404).
days=()
for arg in "$@"; do
  case "$arg" in
    [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]) days+=("$arg") ;;
    [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]..[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9])
      d="${arg%%..*}"; end="${arg##*..}"
      while [ "$d" -le "$end" ]; do
        [ "$(date -d "$d" +%u)" -le 5 ] && days+=("$d")
        d="$(date -d "$d + 1 day" +%Y%m%d)"
      done ;;
    *) echo "bad day '$arg' (want YYYYMMDD or YYYYMMDD..YYYYMMDD)" >&2; exit 1 ;;
  esac
done
urls=()
for day in "${days[@]}"; do
  y="${day:0:4}"; m="${day:4:2}"
  q=$(( (10#$m - 1) / 3 + 1 ))
  urls+=("https://www.sec.gov/Archives/edgar/Feed/$y/QTR$q/$day.nc.tar.gz")
done
exec java -cp "$JAR" org.apache.calcite.adapter.govdata.sec.EdgarFeedUnpacker "$FEED_DIR" "${urls[@]}"
