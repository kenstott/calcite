#!/usr/bin/env bash
# --force-download must target the MinIO raw cache the run reads (GOVDATA_CACHE_DIR), never r2: (#1346).
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$DIR/common.sh"
fail=0
check() { [ "$2" = "$3" ] || { echo "FAIL $1: got '$2' want '$3'"; fail=1; }; }

check s3-path "$(raw_cache_rclone_path s3://govdata-raw-v1 sec/0001234567/0001234567-25-000001/)" \
  "minio:govdata-raw-v1/sec/0001234567/0001234567-25-000001/"
check s3-prefix "$(raw_cache_rclone_path s3://b/pre sec/x/)" "minio:b/pre/sec/x/"
check remote-override "$(GOVDATA_MINIO_REMOTE=mn raw_cache_rclone_path s3://b sec/x/)" "mn:b/sec/x/"
case "$(raw_cache_rclone_path s3://b sec/x/)" in r2:*) echo "FAIL: r2 path"; fail=1 ;; esac
if raw_cache_rclone_path /tmp/cache sec/x/ >/dev/null 2>&1; then echo "FAIL: local dir accepted"; fail=1; fi
# the script must build its path with the helper and keep no r2: string rewrite
if grep -q "r2:" "$DIR/worker-sec-reprocess.sh"; then echo "FAIL: worker-sec-reprocess.sh references r2:"; fail=1; fi
grep -q raw_cache_rclone_path "$DIR/worker-sec-reprocess.sh" || { echo "FAIL: helper unused"; fail=1; }
[ "$fail" -eq 0 ] && echo "PASS"
exit $fail
