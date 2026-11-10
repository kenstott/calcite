#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Licensed under the Business Source License 1.1 (see ../../LICENSE, ../../NOTICE).
"""Fail when a shipped catalog cache lacks a table or view a govdata schema YAML declares.

The cache file is keyed by model.json's content hash, and adding a table to a schema YAML never
changes model.json -- so a cache built before the table existed keeps matching and is served as
the whole catalog (govdata-ops#940: every fisheries table missing). This compares the cache against
the YAMLs instead.

A declared table the warehouse does not hold yet (its ETL has not run) is absent from every
complete catalog walk, so no regenerated cache can contain it. Such tables are named, one
``schema.table`` per line, in the unmaterialized list; a declared table missing from the cache
must be listed there. The list is kept honest both ways: a listed table the cache holds, or one
no YAML declares, also fails, so an entry is dropped as soon as its table is materialized and
the cache regenerated.

    PYTHONPATH=pgwire-calcite/src python3 check_catalog_cache_coverage.py \\
        <cache.pkl> <unmaterialized.txt> <schema-yaml>...
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


def unmaterialized(list_path):
    """The ``schema.table`` entries of the list; blank lines and ``#`` comments are ignored."""
    entries = set()
    with open(list_path) as f:
        for lineno, raw in enumerate(f, 1):
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            schema, dot, table = line.partition(".")
            if not dot or not schema or not table or "." in table:
                raise ValueError(f"{list_path}:{lineno}: expected schema.table, got {line!r}")
            entries.add((schema, table))
    return entries


def _report(header, pairs):
    print(header, file=sys.stderr)
    for schema, table in sorted(pairs):
        print(f"  {schema}.{table}", file=sys.stderr)


def main(argv):
    cache_path, list_path, yaml_paths = argv[1], argv[2], argv[3:]
    with open(cache_path, "rb") as f:
        _, ctx, _ = pickle.load(f)
    present = {(t.schema_name, t.table_name) for t in ctx.tables.values()}
    declared_pairs = {pair for p in yaml_paths for pair in declared(p)}
    listed = unmaterialized(list_path)

    missing = declared_pairs - present - listed
    listed_but_cached = listed & present
    listed_but_undeclared = listed - declared_pairs

    if missing:
        _report(
            f"{cache_path} lacks {len(missing)} declared table(s)/view(s) -- regenerate the cache "
            f"from a complete walk, or, only for a table the warehouse does not hold, list it in "
            f"{list_path}:",
            missing)
    if listed_but_cached:
        _report(
            f"{list_path} lists {len(listed_but_cached)} table(s) the cache holds -- they are "
            f"materialized now; remove them from the list:",
            listed_but_cached)
    if listed_but_undeclared:
        _report(
            f"{list_path} lists {len(listed_but_undeclared)} table(s) no schema YAML declares -- "
            f"remove them from the list:",
            listed_but_undeclared)
    return 1 if (missing or listed_but_cached or listed_but_undeclared) else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
