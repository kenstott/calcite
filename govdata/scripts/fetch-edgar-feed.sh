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
# Disk: the unpacked tree is about 15-20% of the bytes downloaded (a 450 GB year is about 65-85 GB).
# Use a large disk, not /tmp (a RAM-backed tmpfs here): /mnt/wsltmp has several TB free, enough for
# every year at once; /var/tmp/govdata holds about 95 GB and only fits a half year or a quarter.
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
# Days are marked done individually, so a stopped run resumes where it left off: a day is marked done only after its
# whole file was read, and a marked day is skipped. The download is network-bound (about 7 MB/s on
# one stream) and uses a small share of one core, so it runs niced and does not
# draw on the request budget the ETL shares (it is one request per day, outside EdgarRateLimiter).
# The heap is 3 GB, not 1: one entry of a busy day can be 200 MB, and parsing it holds several copies
# (8 days failed with OutOfMemoryError at 1 GB on 2026-10-15).
mkdir -p "$FEED_DIR/.done"
lock="$FEED_DIR/.fetch.lock"
exec 9>"$lock"
flock -n 9 || { echo "another fetch-edgar-feed.sh is already using $FEED_DIR" >&2; exit 1; }
runner=()
command -v nice >/dev/null && runner+=(nice -n 19)
command -v ionice >/dev/null && runner+=(ionice -c3)
# PARALLEL=N runs N days at once (default 1). Days are independent (an accession belongs to one
# day) and each has its own done marker. A failed day (e.g. HTTP 429/403) is reported and left
# unmarked so a rerun retries it; the other days continue.
PARALLEL="${PARALLEL:-1}"
export FEED_DIR JAR
export RUNNER="${runner[*]:-}"
fetch_day() {
  local day="$1"
  if [ -f "$FEED_DIR/.done/$day" ]; then
    echo "[edgar-feed] $day: already unpacked, skipped"
    return 0
  fi
  local y="${day:0:4}" m="${day:4:2}" q
  q=$(( (10#$m - 1) / 3 + 1 ))
  local url="https://www.sec.gov/Archives/edgar/Feed/$y/QTR$q/$day.nc.tar.gz"
  if $RUNNER java -Xmx3g -cp "$JAR" org.apache.calcite.adapter.govdata.sec.EdgarFeedUnpacker \
      "$FEED_DIR" "$url"; then
    touch "$FEED_DIR/.done/$day"
  else
    echo "[edgar-feed] $day: FAILED (left unmarked, rerun retries it)" >&2
    return 1
  fi
}
export -f fetch_day
printf '%s\n' "${days[@]}" | xargs -P "$PARALLEL" -I{} bash -c 'fetch_day {}'
