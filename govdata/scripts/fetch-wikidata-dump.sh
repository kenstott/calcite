#!/usr/bin/env bash
#
# fetch-wikidata-dump.sh <dump_dir> [YYYYMMDD]
#
# Downloads one weekly Wikidata JSON entity dump (CC0, about 160 GB gzip) into <dump_dir>, the
# directory ref.wikidata.dumpDir (WIKIDATA_DUMP_DIR) points the ref wikidata tables at. Without a
# date it takes the newest dated dump whose checksum file lists the gzip JSON dump.
#
# The file is stored once per dump as <dump_dir>/wikidata-<date>-all.json.gz and appears under that
# name only after its MD5 matched the published one. Interrupted downloads resume: rerun the same
# command. The newest WIKIDATA_DUMP_KEEP (default 2) complete dumps are kept.
#
# Use a large local disk outside the ETL raw cache and the Iceberg warehouse, so the dump is never
# part of a bucket sync.
#
# Wikimedia asks for no more than 2-3 concurrent connections; this uses 2 byte-range segments
# (about 6 MB/s together). An HTTP 403 or 429 stops the run at once.
set -euo pipefail

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  echo "Usage: $0 <dump_dir> [YYYYMMDD]" >&2
  exit 1
fi
DUMP_DIR="$1"
DATE="${2:-}"
BASE_URL="${WIKIDATA_DUMP_BASE_URL:-https://dumps.wikimedia.org/wikidatawiki/entities}"
UA="${WIKIDATA_USER_AGENT:-govdata-etl/1.0}"
SEGMENTS=2
KEEP="${WIKIDATA_DUMP_KEEP:-2}"
MAX_RETRIES="${MAX_RETRIES:-100}"
RETRY_DELAY="${RETRY_DELAY:-60}"

log() { echo "[wikidata-dump] $(date -u +%FT%TZ) $*"; }
die() { log "ERROR: $*" >&2; exit 1; }

mkdir -p "$DUMP_DIR"
exec 9>"$DUMP_DIR/.fetch.lock"
flock -n 9 || die "another fetch is already using $DUMP_DIR"

get() { curl -fsSL -A "$UA" --connect-timeout 30 --max-time 120 "$1"; }

if [ -z "$DATE" ]; then
  listing="$(get "$BASE_URL/")" || die "cannot list $BASE_URL/"
  dates="$(grep -oE 'href="[0-9]{8}/"' <<<"$listing" | grep -oE '[0-9]{8}' | sort -r)" \
    || die "no dated dump directories at $BASE_URL/"
  for d in $dates; do
    # A dated directory without a checksum file (HTTP 404) is a dump that is not finished yet.
    code="$(curl -sS -A "$UA" --max-time 120 -o /dev/null -w '%{http_code}' \
      "$BASE_URL/$d/wikidata-$d-md5sums.txt")" || die "cannot probe $d"
    case "$code" in
      200)
        if get "$BASE_URL/$d/wikidata-$d-md5sums.txt" | grep -q " wikidata-$d-all\.json\.gz$"; then
          DATE="$d"; break
        fi ;;
      404) ;;
      *) die "HTTP $code probing dump $d" ;;
    esac
  done
  [ -n "$DATE" ] || die "no complete gzip JSON dump found at $BASE_URL/"
fi

name="wikidata-$DATE-all.json.gz"
url="$BASE_URL/$DATE/$name"
final="$DUMP_DIR/$name"

prune() {
  local old
  # Names carry the dump date, so a reverse name sort is newest first.
  find "$DUMP_DIR" -maxdepth 1 -name 'wikidata-????????-all.json.gz' | sort -r \
    | tail -n +"$((KEEP + 1))" | while read -r old; do
      log "removing old dump $old"
      rm -f -- "$old"
    done
}

if [ -f "$final" ]; then
  log "$name already present"
  prune
  exit 0
fi

sums="$(get "$BASE_URL/$DATE/wikidata-$DATE-md5sums.txt")" || die "cannot read checksums for $DATE"
expected="$(awk -v n="$name" '$2 == n {print $1}' <<<"$sums")"
[ -n "$expected" ] || die "checksum file for $DATE does not list $name"
size="$(curl -fsSIL -A "$UA" --max-time 120 "$url" \
  | tr -d '\r' | awk 'tolower($1) == "content-length:" {n = $2} END {print n}')"
[[ "$size" =~ ^[0-9]+$ ]] || die "no Content-Length for $url"
log "dump $DATE: $size bytes, md5 $expected"

work="$DUMP_DIR/.dl-$DATE"
mkdir -p "$work"
chunk=$(( (size + SEGMENTS - 1) / SEGMENTS ))

fetch_segment() {
  local i="$1" part="$work/part$1" attempt=0
  local from=$(( i * chunk )) to=$(( (i + 1) * chunk - 1 ))
  [ "$to" -lt "$size" ] || to=$(( size - 1 ))
  local want=$(( to - from + 1 )) have rc code
  while :; do
    have=0
    [ -f "$part" ] && have="$(stat -c %s "$part")"
    [ "$have" -eq "$want" ] && return 0
    [ "$have" -lt "$want" ] || die "segment $i is longer than its range ($have > $want): remove $part"
    # curl appends straight to the segment, so a stopped run keeps every byte received. -f keeps
    # an error body out of the file; any other status than 206 (partial content) is cut off again.
    rc=0
    : >"$work/hdr$i"
    curl -fsS -A "$UA" --connect-timeout 30 --speed-limit 10000 --speed-time 300 \
      -r "$(( from + have ))-$to" -D "$work/hdr$i" "$url" >>"$part" || rc=$?
    code="$(awk '/^HTTP/ {c = $2} END {print c}' "$work/hdr$i")"
    if [ "$code" != 206 ]; then
      truncate -s "$have" "$part"
      case "$code" in
        403|429) die "HTTP $code from $url; stopping" ;;
        ''|5??) ;;
        *) die "unexpected HTTP $code for segment $i" ;;
      esac
    fi
    [ "$rc" -eq 0 ] && continue
    attempt=$(( attempt + 1 ))
    [ "$attempt" -le "$MAX_RETRIES" ] || die "segment $i failed after $MAX_RETRIES retries (curl $rc)"
    log "segment $i: curl $rc (HTTP ${code:-none}), retry $attempt in ${RETRY_DELAY}s"
    sleep "$RETRY_DELAY"
  done
}

if [ ! -f "$work/joined" ]; then
  start=$(date +%s)
  pids=()
  for (( i = 0; i < SEGMENTS; i++ )); do
    fetch_segment "$i" & pids+=($!)
  done
  failed=0
  for p in "${pids[@]}"; do wait "$p" || failed=1; done
  [ "$failed" -eq 0 ] || die "download incomplete; rerun to resume"
  log "download finished in $(( $(date +%s) - start ))s"
  for (( i = 1; i < SEGMENTS; i++ )); do
    cat "$work/part$i" >>"$work/part0"
    rm -f "$work/part$i"
  done
  mv "$work/part0" "$work/joined"
fi

log "verifying MD5"
actual="$(md5sum "$work/joined" | awk '{print $1}')"
if [ "$actual" != "$expected" ]; then
  mv "$work/joined" "$work/corrupt"
  die "MD5 mismatch for $name: expected $expected, got $actual (kept as $work/corrupt)"
fi
mv "$work/joined" "$final"
rm -rf -- "$work"
log "complete: $final"
prune
