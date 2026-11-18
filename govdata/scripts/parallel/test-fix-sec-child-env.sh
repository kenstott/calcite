#!/usr/bin/env bash
# The scripts fix-sec.sh and run-scheduled.sh call as children (fetch-submissions-bulk.sh) need
# GOVDATA_TMPDIR in their environment: common.sh must export it (#1315).
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
got=$(env -u GOVDATA_TMPDIR bash -c '. "$1/common.sh"; bash -c '\''printf %s "${GOVDATA_TMPDIR-}"'\''' _ "$here")
[ -n "$got" ] || { echo "FAIL: GOVDATA_TMPDIR not exported by common.sh" >&2; exit 1; }
echo "PASS: child sees GOVDATA_TMPDIR=$got"
