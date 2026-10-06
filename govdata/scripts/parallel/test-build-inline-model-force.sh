#!/usr/bin/env bash
# Tests that build_inline_model carries GOVDATA_FORCE_REPROCESS_TABLES into the sec model as the
# forceReprocessTables operand (the SEC materializer deletes re-staged accessions first), and only
# there.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }
model() { # schema, force-tables
  ( source "$HERE/common.sh" >/dev/null 2>&1
    export GOVDATA_PARQUET_DIR="s3://test-bucket"
    GOVDATA_FORCE_REPROCESS_TABLES="$2" build_inline_model "$1" "" 2>/dev/null )
}

m="$(model sec "financial_line_items,filing_metadata")"
[[ "$m" == *'"forceReprocessTables":["financial_line_items","filing_metadata"]'* ]] \
  && ok "sec model carries the forced tables" || bad "sec model missing forceReprocessTables: $m"
python3 -c "import json,sys; json.loads(sys.argv[1])" "$m" 2>/dev/null \
  && ok "sec model with forced tables is valid JSON" || bad "sec model is not valid JSON"

m="$(model sec "")"
[[ "$m" != *forceReprocessTables* ]] && ok "no force request, no operand" || bad "operand present without a request"

m="$(model econ "gdp")"
[[ "$m" != *forceReprocessTables* ]] && ok "other schemas are untouched" || bad "operand leaked into econ"

m="$(model sec " mda_sections , risk_factor_sections ")"
[[ "$m" == *'"forceReprocessTables":["mda_sections","risk_factor_sections"]'* ]] \
  && ok "whitespace in the list is trimmed" || bad "whitespace not trimmed: $m"
exit $fail
