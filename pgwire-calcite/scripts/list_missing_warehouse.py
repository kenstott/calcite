#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Licensed under the Business Source License 1.1 (see ../../LICENSE, ../../NOTICE).
"""Read-only diagnostic: for each table a govdata schema YAML declares and the catalog cache lacks,
list <warehouse>/<schema>/<table>/ and its metadata/ with `aws s3 ls`, printing what exists.
Evidence for pgwire-govdata/unmaterialized-tables.txt, or for a defect in the walk.

    GOVDATA_PARQUET_DIR=s3://... AWS_ENDPOINT_OVERRIDE=... \\
    PYTHONPATH=pgwire-calcite/src:pgwire-calcite/scripts python3 list_missing_warehouse.py <cache.pkl> <schema-yaml>...
"""
import os
import pickle
import subprocess
import sys

from check_catalog_cache_coverage import declared


def _ls(uri, endpoint):
    cmd = ["aws", "s3", "ls", uri]
    if endpoint:
        cmd += ["--endpoint-url", endpoint]
    r = subprocess.run(cmd, capture_output=True, text=True)
    out = r.stdout.strip() or "(empty)"
    err = f" stderr: {r.stderr.strip()}" if r.stderr.strip() else ""
    print(f"  $ aws s3 ls {uri} -> exit {r.returncode}{err}")
    for line in out.splitlines()[:40]:
        print(f"    {line}")


def main(argv):
    with open(argv[1], "rb") as f:
        _, ctx, _ = pickle.load(f)
    present = {(t.schema_name, t.table_name) for t in ctx.tables.values()}
    pairs = {pair for p in argv[2:] for pair in declared(p)}
    warehouse = os.environ["GOVDATA_PARQUET_DIR"].rstrip("/")
    endpoint = os.environ.get("AWS_ENDPOINT_OVERRIDE")
    for schema, table in sorted(pairs - present):
        print(f"{schema}.{table}")
        _ls(f"{warehouse}/{schema}/{table}/", endpoint)
        _ls(f"{warehouse}/{schema}/{table}/metadata/", endpoint)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
