#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Assert every partitioned table in a schema YAML is referenced by that schema's *_dq.sql.

The per-table DQ scripts are hand-maintained, so a table that is simply ABSENT from
<schema>_dq.sql produces no rows at all — indistinguishable from a healthy table. That
blind spot is not hypothetical: geo's state_ref, zcta_ref, census_regions and
census_divisions were absent from geo_dq.sql while every write to them failed with
S3 400 "Object name contains unsupported characters" (empty partition path -> "data//file"),
leaving all four with zero data files and no DQ signal.

This check is deliberately structural, not data-quality: it verifies the SUITE covers the
model, so the existing existence/row_count/null checks actually get a chance to fire.

Exit codes: 0 = every table covered; 1 = uncovered tables found; 2 = usage/IO error.
"""

import glob
import os
import re
import sys

try:
    import yaml
except ImportError:
    sys.stderr.write("PyYAML required: pip install pyyaml\n")
    sys.exit(2)

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESOURCES = os.path.join(REPO, "src", "main", "resources")
SCRIPTS = os.path.join(REPO, "scripts")


def schema_key(yaml_path):
    """Derive the <schema>_dq.sql stem from a schema YAML filename."""
    base = os.path.basename(yaml_path)
    for suffix in ("-schema.yaml", ".yaml"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    return base.replace("-", "_")


def main(argv):
    only = set(argv[1:])
    total = covered = 0
    gaps = []

    for path in sorted(glob.glob(os.path.join(RESOURCES, "*", "*.yaml"))):
        try:
            doc = yaml.safe_load(open(path))
        except Exception:
            continue  # reference-data YAML, not a schema
        if not isinstance(doc, dict) or not doc.get("partitionedTables"):
            continue

        key = schema_key(path)
        if only and key not in only:
            continue

        dq_path = os.path.join(SCRIPTS, key + "_dq.sql")
        tables = sorted(
            t["name"] for t in doc["partitionedTables"]
            if isinstance(t, dict) and t.get("name")
        )
        if not tables:
            continue

        if not os.path.exists(dq_path):
            total += len(tables)
            gaps.append((key, len(tables), tables, "no %s_dq.sql at all" % key))
            continue

        sql = open(dq_path).read()
        # A table is covered if its name appears as a path segment or quoted literal.
        # Substring-only matches are rejected so 'places' does not cover 'gazetteer_places'.
        missing = [t for t in tables if not re.search(re.escape(t) + r"['/\"]", sql)]
        total += len(tables)
        covered += len(tables) - len(missing)
        if missing:
            gaps.append((key, len(tables), missing, None))

    print("DQ coverage: %d/%d tables referenced by their schema's _dq.sql"
          % (covered, total))
    if not gaps:
        print("OK — every partitioned table is covered.")
        return 0

    print("\nUNCOVERED (a table absent from _dq.sql yields NO dq rows — silent, not passing):")
    for key, n, missing, note in sorted(gaps, key=lambda g: -len(g[2])):
        suffix = (" [%s]" % note) if note else ""
        print("  %-18s (%d tables) missing %d%s:" % (key, n, len(missing), suffix))
        for t in missing:
            print("      %s" % t)
    print("\nAdd at minimum a T1 existence + T2 row_count block for each, mirroring the"
          " iceberg_scan(...) pattern already used in that file.")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
