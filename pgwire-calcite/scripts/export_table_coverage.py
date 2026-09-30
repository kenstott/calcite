#!/usr/bin/env python3
"""Write the table-coverage JSON that pgwire-calcite's --table-coverage-file reads.

Usage: export_table_coverage.py OUT.json SCHEMA_YAML [SCHEMA_YAML ...]

Each table with an ``observedCoverage.rowCount`` yields ``<schema>.<table>`` ->
``{row_count, partition_columns}``; the schema name is the YAML's ``schemaName`` default.
Partition columns other than the ``type``/``frequency`` discriminators are the ones a filter
can narrow a scan with.
"""
import json
import re
import sys

import yaml

_DISCRIMINATORS = {"type", "frequency"}


def main(out_path, yaml_paths):
    out = {}
    for path in yaml_paths:
        with open(path, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
        schema = re.fullmatch(r"\$\{[A-Za-z_0-9]+:([^}]*)\}", doc["schemaName"])
        schema = schema.group(1) if schema else doc["schemaName"]
        for table in (doc.get("partitionedTables") or []) + (doc.get("tables") or []):
            cov = table.get("observedCoverage") or {}
            if "rowCount" not in cov:
                continue
            cols = [
                c["name"]
                for c in (table.get("partitions") or {}).get("columnDefinitions", [])
                if c["name"] not in _DISCRIMINATORS
            ]
            if not cols:
                continue
            out[f"{schema}.{table['name']}"] = {
                "row_count": cov["rowCount"],
                "partition_columns": cols,
            }
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=1, sort_keys=True)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2:])
