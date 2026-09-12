#!/usr/bin/env python3
"""
check-datalag-scoping.py  <schema_yaml>  <table_name>  <pipeline_start_year>  <pipeline_end_year>

Reads the named table's `dimensions.year.dataLag` from a GovData schema YAML (resolving YAML
anchors/aliases, e.g. `year: *nbi_year_range`, the same way several tables share one year-range
block) and prints the mapping between the pipeline years a caller passes to force-reprocess.sh's
--start/--end and the effective/data years those combos actually fetch and write
(effective_year = pipeline_year - dataLag).

Confirmed live 2026-09-12 (kenstott/govdata-ops#232, #233): a caller who thinks in terms of the
data years they want (e.g. "I want years 2017-2018") and passes them directly as --start/--end
silently rewrites the WRONG partition whenever dataLag > 0 — force-reprocess.sh completes with
exit 0 and an advanced tracker, giving no other signal that the intended partition was untouched.

Prints nothing and exits 0 when dataLag is absent or zero (pipeline year == effective year, no
translation needed — most tables). Prints the mapping and exits 0 (informational only; never
blocks a run) when dataLag > 0.
"""
import sys

try:
    import yaml
except ImportError:
    # PyYAML missing is not this check's problem to fail loudly over — force-reprocess.sh treats
    # this script as advisory and should not abort a real remediation run over a missing dependency.
    sys.exit(0)


def find_table(doc, table_name):
    section = doc.get("partitionedTables")
    if isinstance(section, list):
        for t in section:
            if isinstance(t, dict) and t.get("name") == table_name:
                return t
    return None


def main():
    if len(sys.argv) != 5:
        sys.exit(0)  # advisory only — never block on a malformed invocation
    schema_yaml_path, table_name, start_str, end_str = sys.argv[1:5]

    try:
        start_year = int(start_str)
        end_year = int(end_str)
    except ValueError:
        sys.exit(0)

    try:
        with open(schema_yaml_path) as f:
            doc = yaml.safe_load(f)
    except (OSError, yaml.YAMLError):
        sys.exit(0)

    table = find_table(doc, table_name)
    if table is None:
        sys.exit(0)

    year_dim = (table.get("dimensions") or {}).get("year")
    if not isinstance(year_dim, dict):
        sys.exit(0)

    data_lag = year_dim.get("dataLag")
    if not data_lag:
        sys.exit(0)

    eff_start = start_year - data_lag
    eff_end = end_year - data_lag
    print("  NOTE: '{}' has dataLag={} — pipeline years {}-{} you requested map to "
          "EFFECTIVE/DATA years {}-{} (effective_year = pipeline_year - dataLag).".format(
              table_name, data_lag, start_year, end_year, eff_start, eff_end))
    print("        If you meant to target data years {}-{} directly, pass "
          "--start {} --end {} instead.".format(
              start_year, end_year, start_year + data_lag, end_year + data_lag))


if __name__ == "__main__":
    main()
