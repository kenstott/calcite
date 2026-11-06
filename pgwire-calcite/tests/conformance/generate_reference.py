# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Record what a real PostgreSQL serves, as the reference the catalog conformance test reads.

For every pg_catalog and information_schema relation: its columns in order, each with the
type's name and OID. Run against a stock PostgreSQL of the level the server declares
(server_version 14.x; pg_catalog columns only ever grow, so a later release is a superset):

    python generate_reference.py --pg "<dsn>" --out reference/pg16-catalog.json

The reference is data, committed. Regenerate it only to move to a new PostgreSQL release, and
review the diff: a column that appears is a new thing the server may need to serve or to accept
as a deviation (accepted_deviations.yaml).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import psycopg

RELATIONS_SQL = """
SELECT n.nspname, c.relname, a.attnum, a.attname, a.atttypid, t.typname
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
WHERE n.nspname IN ('pg_catalog', 'information_schema') AND c.relkind IN ('r', 'v', 'm', 'f')
ORDER BY n.nspname, c.relname, a.attnum
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--pg", required=True, help="libpq DSN of a stock PostgreSQL")
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    with psycopg.connect(args.pg, autocommit=True) as conn:
        version = conn.execute("SHOW server_version").fetchone()[0]
        relations: dict[str, list[dict]] = {}
        for schema, rel, _num, col, oid, typname in conn.execute(RELATIONS_SQL):
            relations.setdefault(f"{schema}.{rel}", []).append(
                {"name": col, "type": typname, "oid": oid}
            )
    args.out.write_text(
        json.dumps({"postgres_version": version, "relations": relations}, indent=1) + "\n"
    )
    print(f"{version}: {len(relations)} relations -> {args.out}")


if __name__ == "__main__":
    main()
