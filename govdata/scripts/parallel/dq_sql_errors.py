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
"""Report the statements of a DQ run that raised an error, as CSV on stdout.

A statement that errors inserts nothing into dq_results, so its checks are absent from the
results rather than failed. The DQ runner adds one failing result per row printed here:
one per (table, error message), with the number of statements that raised it.

Only the error's first line is reported. The SQL snippet DuckDB prints under it is read to
name the table and is never copied, because the DQ SQL carries substituted credentials.

In a --tables run (the fourth argument) an error naming a table outside the scope produces no
row: dq_scope.py has already left out the statements that read only such tables, so it comes
from a cross-table check that cannot run. It is listed on stderr. An unresolved error counts.

Usage: dq_sql_errors.py <duckdb-log> <dq-sql> <duckdb-exit-code> [<comma-separated-tables>]
"""

import collections
import csv
import os
import re
import sys

ERROR_LINE = re.compile(r"^(?:[A-Za-z]+ ){0,4}Error: .*")
TABLE_PATH = re.compile(r"iceberg_scan\(\s*'s3://[^/']+/([\w-]+/\w+)")
# Overlapping matches, so the bucket in s3://<bucket>/<schema>/<table> does not consume <schema>.
SNIPPET_PATH = re.compile(r"(?<![\w-])(?=([\w-]+/\w+)(\.\.\.)?)")
UNRESOLVED = "(unresolved)"
SECRET_VARS = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")


def table_in(snippet, known):
    """Table named by a DuckDB 'LINE n:' snippet, which may cut the path short with '...'."""
    for path, ellipsis in SNIPPET_PATH.findall(snippet):
        if path in known:
            return path.split("/", 1)[1]
        if ellipsis:
            matches = [k for k in known if k.startswith(path)]
            if len(matches) == 1:
                return matches[0].split("/", 1)[1]
    return UNRESOLVED


def errors(log_lines, known):
    """Counter of (table, message) over every error block in the log, in log order."""
    found = collections.OrderedDict()
    for i, line in enumerate(log_lines):
        if not ERROR_LINE.match(line):
            continue
        table = UNRESOLVED
        for follower in log_lines[i + 1:]:
            if ERROR_LINE.match(follower):
                break
            if follower.startswith("LINE "):
                table = table_in(follower, known)
                break
        message = line.strip()
        for name in SECRET_VARS:
            if os.environ.get(name):
                message = message.replace(os.environ[name], "<redacted>")
        key = (table, message[:300])
        found[key] = found.get(key, 0) + 1
    return found


def main(argv):
    if len(argv) not in (4, 5):
        sys.stderr.write(__doc__)
        return 2
    log_path, sql_path, exit_code = argv[1:4]
    scope = {name.strip() for name in argv[4].split(",")} if len(argv) == 5 else None
    with open(sql_path, encoding="utf-8") as handle:
        known = set(TABLE_PATH.findall(handle.read()))
    with open(log_path, encoding="utf-8", errors="replace") as handle:
        found = errors(handle.read().splitlines(), known)
    if not found:
        found = {(UNRESOLVED, "DuckDB exited with code %s without printing an error line; "
                              "see %s" % (exit_code, os.path.basename(log_path))): 1}
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(["table_name", "value", "detail"])
    for (table, message), count in found.items():
        if scope is not None and table != UNRESOLVED and table not in scope:
            sys.stderr.write("not run: %d statement(s) need %s, outside --tables: %s\n"
                             % (count, table, message))
            continue
        writer.writerow([table, count, message])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
