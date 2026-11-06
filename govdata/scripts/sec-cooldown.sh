#!/usr/bin/env bash
# Host-wide sec.gov cool-down shared with EdgarRateLimiter, sourced by every script that calls sec.gov
# directly (fetch-edgar-feed.sh, fetch-submissions-bulk.sh, fetch-13f-bulk.sh, run-scheduled.sh).
#
# The marker holds the epoch millis until which no process on this host may call sec.gov, in
# <java.io.tmpdir>/calcite-ratelimit-sec.gov.blocked. The pool starts the workers with
# -Djava.io.tmpdir=$GOVDATA_TMPDIR (parallel/common.sh), so the marker path comes from the same
# variable. There is no default: a guessed directory is a second marker the workers never read.
# This file is standalone (sourcing parallel/common.sh would apply its /var/tmp/govdata default).

SEC_COOLDOWN_EXIT=75        # exit status of a script that stopped for the cool-down
SEC_COOLDOWN_SECS="${SEC_COOLDOWN_SECS:-1800}"

sec_cooldown_init() {
  : "${GOVDATA_TMPDIR:?GOVDATA_TMPDIR is not set: it is the directory the ETL workers use as java.io.tmpdir, which holds the sec.gov cool-down marker (export the value parallel/common.sh uses, /var/tmp/govdata by default)}"
  SEC_BLOCK_FILE="$GOVDATA_TMPDIR/calcite-ratelimit-sec.gov.blocked"
}

# Seconds left on the cool-down (0 when none).
sec_cooldown_remaining() {
  local until_ms now_ms
  [ -f "$SEC_BLOCK_FILE" ] || { echo 0; return 0; }
  until_ms=$(tr -d '[:space:]' < "$SEC_BLOCK_FILE")
  now_ms=$(( $(date +%s) * 1000 ))
  if [ "$until_ms" -gt "$now_ms" ]; then echo $(( (until_ms - now_ms + 999) / 1000 )); else echo 0; fi
}

# Returns 1 and names the marker and its end time when a cool-down is in effect.
sec_cooldown_refuse() {
  local who="$1" left until_ms
  left=$(sec_cooldown_remaining)
  [ "$left" -gt 0 ] || return 0
  until_ms=$(tr -d '[:space:]' < "$SEC_BLOCK_FILE")
  echo "[$who] sec.gov cool-down in effect until $(date -d "@$((until_ms / 1000))" -u +%FT%TZ) ($SEC_BLOCK_FILE); not calling sec.gov" >&2
  return 1
}

# sec_cooldown_block <retry-after-header-value>: cool down for the longer of the default and a
# numeric Retry-After (seconds).
sec_cooldown_block() {
  local secs="$SEC_COOLDOWN_SECS"
  case "${1:-}" in ''|*[!0-9]*) ;; *) [ "$1" -gt "$secs" ] && secs="$1" ;; esac
  printf '%s' $(( ($(date +%s) + secs) * 1000 )) > "$SEC_BLOCK_FILE"
}

# sec_retry_after <curl -D header file>: the last numeric Retry-After value, or nothing.
sec_retry_after() {
  tr -d '\r' < "$1" | awk 'tolower($1)=="retry-after:"{print $2}' | tail -1
}

# sec_cooldown_on_status <who> <http status> <header file>: on 403/429 records the cool-down, says so
# naming the status and returns 1; any other status returns 0.
sec_cooldown_on_status() {
  local who="$1" code="$2" hdr="$3" ra
  case "$code" in 403|429) ;; *) return 0 ;; esac
  ra=$(sec_retry_after "$hdr")
  sec_cooldown_block "$ra"
  echo "[$who] HTTP $code from sec.gov, stopping (no retry); cool-down recorded${ra:+, Retry-After $ra}" >&2
  return 1
}

# sec_user_agent_init: sets SEC_UA to the declared sec.gov User-Agent. The only source is
# edgarUserAgent in sec-schema.yaml, resolved by the govdata jar (GOVDATA_JAR); there is no default
# here. Fails, naming what is missing, before any request if it cannot be obtained.
sec_user_agent_init() {
  : "${GOVDATA_JAR:?GOVDATA_JAR is not set: the SEC User-Agent is resolved from sec-schema.yaml by the govdata jar (set it to the shadow jar)}"
  [ -f "$GOVDATA_JAR" ] || { echo "GOVDATA_JAR not found: $GOVDATA_JAR (needed to resolve the SEC User-Agent)" >&2; return 1; }
  SEC_UA=$(java -cp "$GOVDATA_JAR" org.apache.calcite.adapter.govdata.sec.SecUserAgentMain) \
    && [ -n "$SEC_UA" ] || { echo "could not resolve the SEC User-Agent from $GOVDATA_JAR (edgarUserAgent in sec-schema.yaml)" >&2; return 1; }
}
