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
"""Fail when a govdata view passes a stored-text integer column through without declaring it.

A partition column a table declares as an integer may be stored as text (``year=2023`` from the
hive path). The DuckDB engine serves the table's column as the integer, but a view runs inside
DuckDB over the stored text, so its copy of the column reads as text unless the view declares it
in its own ``columnDefinitions`` (GOV-075):

    - name: disasters_storm_impact_by_county
      columnDefinitions:
        - {name: year, type: INTEGER, storedDigits: 4}

A stored-text integer column is a ``partitions.columnDefinitions`` entry of an integer type whose
column the published table stores as ``string`` (``duckdb/seed/iceberg-schema-cache.json``). A
view passes one through when a top-level select item is that bare column (``s.year`` or
``s.year AS term``). Every view must parse, after its ``${VAR:default}`` placeholders take their
defaults; a view that does not is named, since it cannot be checked.

Exit codes: 0 every such column is declared; 1 an undeclared, mis-declared or unparseable view;
2 usage/IO error.
"""

import glob
import json
import os
import re
import sys

try:
    import sqlglot
    from sqlglot import exp
    from sqlglot.errors import ParseError
    import yaml
except ImportError as e:
    sys.stderr.write("PyYAML and sqlglot required: pip install pyyaml sqlglot (%s)\n" % e)
    sys.exit(2)

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESOURCES = os.path.join(REPO, "src", "main", "resources")
SCHEMA_CACHE = os.path.join(RESOURCES, "duckdb", "seed", "iceberg-schema-cache.json")
INTEGER_TYPES = {"tinyint", "smallint", "int", "integer", "bigint"}
PLACEHOLDER = re.compile(r"\$\{[A-Za-z0-9_]+:([^}]*)\}")


def schema_name(declared):
    """The schema a YAML's ``schemaName`` resolves to: ``${SCHEMA_NAME:fiscal}`` is fiscal."""
    return PLACEHOLDER.sub(r"\1", declared)


def stored_types():
    """(schema, table) -> {column: Iceberg type} of every published table."""
    with open(SCHEMA_CACHE) as f:
        tables = json.load(f)["tables"]
    result = {}
    for path, entry in tables.items():
        parts = path.rstrip("/").split("/")
        fields = json.loads(entry["schemaJson"])["fields"]
        result[(parts[-2], parts[-1])] = {f["name"]: f["type"] for f in fields}
    return result


def load(path):
    with open(path) as f:
        return yaml.safe_load(f)


def main():
    stored = stored_types()
    schemas = []
    for path in sorted(glob.glob(os.path.join(RESOURCES, "*", "*-schema.yaml"))):
        doc = load(path)
        schemas.append((os.path.relpath(path, REPO), schema_name(doc["schemaName"]), doc))

    # (schema, table) -> {column: (declared type, storedDigits)} for stored-text integer columns
    text_ints = {}
    for _, schema, doc in schemas:
        for table in doc.get("partitionedTables") or []:
            published = stored.get((schema, table["name"]), {})
            for d in (table.get("partitions") or {}).get("columnDefinitions") or []:
                if (isinstance(d, dict) and str(d.get("type")).lower() in INTEGER_TYPES
                        and published.get(d["name"]) == "string"):
                    text_ints.setdefault((schema, table["name"]), {})[d["name"]] = (
                        d["type"], d.get("storedDigits"))

    problems = []
    checked = 0
    for path, schema, doc in schemas:
        for view in doc.get("views") or []:
            name = view.get("name")
            sql = PLACEHOLDER.sub(r"\1", view.get("sql") or "")
            where = "%s %s.%s" % (path, schema, name)
            try:
                tree = sqlglot.parse_one(sql, read="duckdb")
            except ParseError as e:
                problems.append("%s: does not parse, so cannot be checked: %s"
                                % (where, str(e).splitlines()[0]))
                continue
            checked += 1
            select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
            if select is None:
                continue
            # The tables of the top-level FROM and JOINs (sqlglot 30 names the FROM arg "from_").
            sources = {}
            for clause in [select.args.get("from_")] + list(select.args.get("joins") or []):
                if clause is None:
                    continue
                for t in clause.find_all(exp.Table):
                    sources[t.alias_or_name] = (t.db or schema, t.name)
            declared = {d["name"]: d for d in view.get("columnDefinitions") or []
                        if isinstance(d, dict)}
            for item in select.expressions:
                column = item.this if isinstance(item, exp.Alias) else item
                if not isinstance(column, exp.Column):
                    continue
                candidates = ([sources.get(column.table)] if column.table
                              else list(sources.values()))
                for source in candidates:
                    if source is None:
                        continue
                    passed = text_ints.get(source, {}).get(column.name)
                    if passed is None:
                        continue
                    out = item.alias_or_name
                    want_type, want_digits = passed
                    have = declared.get(out)
                    origin = "%s.%s.%s" % (source[0], source[1], column.name)
                    if have is None:
                        problems.append(
                            "%s: column %s passes through %s, stored as text and declared %s;"
                            " declare it: columnDefinitions: [{name: %s, type: %s%s}]"
                            % (where, out, origin, want_type, out, want_type,
                               "" if want_digits is None else ", storedDigits: %d" % want_digits))
                    elif (str(have.get("type")).lower() != str(want_type).lower()
                          or have.get("storedDigits") != want_digits):
                        problems.append(
                            "%s: column %s declares type %s, storedDigits %s; %s is %s,"
                            " storedDigits %s" % (where, out, have.get("type"),
                                                  have.get("storedDigits"), origin, want_type,
                                                  want_digits))
                    break
    for p in problems:
        print(p)
    print("%d views checked; %d problems" % (checked, len(problems)))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
