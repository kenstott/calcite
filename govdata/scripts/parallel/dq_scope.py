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
"""Narrow a DQ script to the tables of a --tables run, as SQL on stdout.

A statement is left out when every table it reads is outside the scope, when it reads a view
or temp table that was left out, or when it creates one from a table outside the scope. A
statement naming tables on both sides of the scope stays in: dq_sql_errors.py decides what
its failure means. Results labelled with an out-of-scope table are deleted at the end.

Left-out statements that also read an in-scope table are listed on stderr.

Usage: dq_scope.py <dq-sql> <comma-separated-tables>
"""

import re
import sys

TOKEN = re.compile(
    r"(?P<comment>--[^\n]*|/\*.*?\*/)"
    r"|(?P<string>'(?:[^']|'')*')"
    r"|(?P<end>;)"
    r"|(?P<code>\"(?:[^\"]|\"\")*\"|[^-/'\";]+|.)",
    re.S)
PATH = r"s3://\$\{GOVDATA_DQ_BUCKET\}/[\w-]+/(\w+)"
ANY_PATH = re.compile(PATH)
SCAN_PATH = re.compile(r"iceberg_scan\(\s*'" + PATH)
CREATED = re.compile(
    r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:(?:TEMP|TEMPORARY)\s+)?(?:TABLE|VIEW|MACRO)\s+"
    r"(?:IF\s+NOT\s+EXISTS\s+)?(\w+)", re.I)


def statements(sql):
    """(text, text without comments, text without comments or strings) per statement."""
    text, plain, code = [], [], []
    for match in TOKEN.finditer(sql):
        kind, value = match.lastgroup, match.group()
        text.append(value)
        plain.append(" " if kind == "comment" else value)
        code.append(" " if kind == "comment" else "''" if kind == "string" else value)
        if kind == "end":
            yield "".join(text), "".join(plain), "".join(code)
            text, plain, code = [], [], []
    if text:
        yield "".join(text), "".join(plain), "".join(code)


def narrow(sql, scope):
    """(kept statement texts, out-of-scope tables, [(in-scope tables, missing tables)])."""
    kept, outside, not_run = [], set(), []
    unbuilt = {}  # name of a left-out view or temp table -> out-of-scope tables it needed
    for text, plain, code in statements(sql):
        tables = set(ANY_PATH.findall(plain))
        outside |= tables - scope
        needed = set()
        for name, missing in unbuilt.items():
            if re.search(r"(?<![\w.])%s(?!\w)" % re.escape(name), code, re.I):
                needed |= missing
        created = CREATED.match(code)
        if created:
            needed |= set(SCAN_PATH.findall(plain)) - scope
        if needed or (tables and not tables & scope):
            missing = needed or tables
            if created:
                unbuilt[created.group(1)] = missing
            if tables & scope:
                not_run.append((sorted(tables & scope), sorted(missing)))
            continue
        kept.append(text)
    return kept, outside, not_run


def main(argv):
    if len(argv) != 3:
        sys.stderr.write(__doc__)
        return 2
    with open(argv[1], encoding="utf-8") as handle:
        sql = handle.read()
    scope = {name.strip() for name in argv[2].split(",") if name.strip()}
    if not scope & set(ANY_PATH.findall(sql)):
        sys.stderr.write("ERROR: %s has no check reading any of the --tables: %s\n"
                         % (argv[1], ", ".join(sorted(scope))))
        return 1
    kept, outside, not_run = narrow(sql, scope)
    for in_scope, missing in not_run:
        sys.stderr.write("not run: a check reading %s needs %s, outside --tables\n"
                         % (", ".join(in_scope), ", ".join(missing)))
    sys.stdout.write("".join(kept))
    if outside:
        sys.stdout.write("\nDELETE FROM dq_results WHERE tbl IN (%s);\n"
                         % ", ".join("'%s'" % name for name in sorted(outside)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
