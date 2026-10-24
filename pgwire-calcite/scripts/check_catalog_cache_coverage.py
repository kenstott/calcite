#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Licensed under the Business Source License 1.1 (see ../../LICENSE, ../../NOTICE).
"""Fail when a shipped catalog cache lacks a table or view a govdata schema YAML declares.

A cache is written only from a complete walk, but a walk sees only the tables the warehouse
serves; a table a schema YAML declares and the walk did not reach would be missing from the
catalog every install serves. This compares the cache against the YAMLs.

    PYTHONPATH=pgwire-calcite/src python3 check_catalog_cache_coverage.py <cache.pkl> <schema-yaml>...
"""
import pickle
import re
import sys

import yaml

_VAR = re.compile(r"\$\{[A-Za-z0-9_]+:([^}]*)\}")
_DECLARING_KEYS = ("tables", "partitionedTables", "views")


def declared(yaml_path):
    with open(yaml_path) as f:
        doc = yaml.safe_load(f)
    schema = _VAR.sub(r"\1", str(doc["schemaName"]))
    return {
        (schema, t["name"])
        for key in _DECLARING_KEYS
        for t in (doc.get(key) or [])
    }


def main(argv):
    cache_path, yaml_paths = argv[1], argv[2:]
    with open(cache_path, "rb") as f:
        _, ctx, _ = pickle.load(f)
    present = {(t.schema_name, t.table_name) for t in ctx.tables.values()}
    missing = sorted(
        pair for p in yaml_paths for pair in declared(p) if pair not in present
    )
    if missing:
        print(f"{cache_path} lacks {len(missing)} declared table(s)/view(s):", file=sys.stderr)
        for schema, table in missing:
            print(f"  {schema}.{table}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
