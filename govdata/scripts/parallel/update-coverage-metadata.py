#!/usr/bin/env python3
#
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
#
# Surgical text-level editor for one table's `observedCoverage:` block in a govdata
# schema YAML. Deliberately does NOT parse+re-dump the file with a YAML library — these
# files are hand-authored with heavy prose comments and folded scalars; a round-trip
# through PyYAML's dumper reformats the whole file and destroys that. Instead this finds
# the target table's `pattern:` line by exact text match and inserts/replaces a single,
# clearly-marked block immediately after it, leaving every other byte in the file alone.
#
# Two modes.
#
#   Edit mode (flat flags: --file --table --min-year ...): write one block from values the
#   caller already measured. Used by update-coverage-metadata.sh for `year=` partitioned tables.
#
#   Measure mode (--measure): for every table that declares a table-level `timeColumn:`, measure
#   the observed first/last year, distinct years, contiguity and row count from the table's
#   Iceberg data and write the same block. The time column can be any of three kinds, and the
#   year is defined for each as follows (anything else fails loudly, nothing is guessed):
#
#     integer     the value is the year; it must be a four-digit year (1000..9999).
#     DATE / TIMESTAMP (with or without time zone; zone values are read in UTC)
#                 the calendar year of the value.
#     VARCHAR     one of these exact forms, per value:
#                   YYYY                 the year itself
#                   YYYY-MM-DD[ or T...] an ISO date or timestamp: its calendar year
#                   YYYY-YYYY            a span label: its END year (2019-2020 -> 2020)
#                   YYYYMMDD             a compact date: its calendar year
#                   YYYY-YY / YYYY-MM    two forms with the same shape, told apart by the column:
#                                        a school/fiscal-year label (2019-20 -> its END year 2020,
#                                        1999-00 -> 2000; YY must be start+1 mod 100) or a year-month
#                                        (2015-09 -> 2015; MM must be 01..12). A value that is only
#                                        one of the two (2015-09 is not a label, 2019-20 is not a
#                                        month) decides the whole column; a column holding both
#                                        kinds, or only values valid as either (2011-12), is
#                                        ambiguous and aborts rather than being guessed at.
#
#   Every kind: a year before 1000, or more than 10 years after the check date, is a source data
#   error (a two-digit year stored as 0013, a typo like 3011), not an observation. Reporting it
#   would publish a first/last year no caller could trust, so the table fails to measure and the
#   offending year is named; the defect belongs to the table's source, not to this script.
#
#   A table whose source holds out-of-range dates (typos, sentinels, or legitimately distant
#   projections) declares a table-level `coverageWindow:` next to its `timeColumn:` with minYear,
#   maxFutureYears and reason, all three required. The window replaces the default plausibility
#   rule for that table: rows whose year falls outside [minYear, check year + maxFutureYears] are
#   left out of minYear/maxYear/distinctYears/years, and the block records how many (excludedRows)
#   and the kept range (excludedOutsideYears). The rows stay in the table; only the coverage
#   summary skips them. A window that leaves no row to measure fails the table.
#
#   VARCHAR notes: a NULL value is no observation and is skipped. An empty string, or any other
#                 shape, aborts the measurement of that table with the offending value.
#
#   The end year is chosen for span labels because a school year 2019-20 is observed across the
#   spring of 2020 and is conventionally named by the year it ends; it is also the year a caller
#   asking "what is the latest year" expects 2025-26 to count toward.
#
# Exit codes (edit mode): 0 = updated (or already up to date), 3 = table has no `pattern:` line (no
# year partition to report on — not an error, just nothing to do), 1 = table not found /
# other error.

import argparse
import csv
import datetime
import glob
import io
import os
import re
import subprocess
import sys

MARKER = "# auto-generated by update-coverage-metadata.sh — do not hand-edit"


def build_block(min_year, max_year, distinct_years, contiguous, years, row_count, checked_at,
                empty=False, excluded=None):
    lines = [f"    observedCoverage:  {MARKER}"]
    if empty:
        # A readable table holding no rows. The year fields are omitted rather than zeroed:
        # there is no observed year coverage to report, and a minYear of 0 (or of the table's
        # declared range) would be an invented value. Consumers already treat a block without
        # minYear/maxYear as "no coverage to summarise".
        lines.append(f"      rowCount: {row_count}")
        lines.append(f"      checkedAt: \"{checked_at}\"")
        return lines
    lines.append(f"      minYear: {min_year}")
    lines.append(f"      maxYear: {max_year}")
    lines.append(f"      distinctYears: {distinct_years}")
    lines.append(f"      contiguous: {'true' if contiguous else 'false'}")
    if not contiguous and years:
        year_list = ", ".join(str(y) for y in years)
        lines.append(f"      years: [{year_list}]")
    lines.append(f"      rowCount: {row_count}")
    if excluded is not None:
        # A declared coverageWindow was applied: say how many rows it left out of the years above
        # and which years it kept, so the block never reads as the whole table.
        lines.append(f"      excludedRows: {excluded['rows']}")
        lines.append(f"      excludedOutsideYears: [{excluded['min_year']}, {excluded['max_year']}]")
    lines.append(f"      checkedAt: \"{checked_at}\"")
    return lines


def indent_of(line):
    return len(line) - len(line.lstrip(" "))


class NoAnchor(Exception):
    """The table has no line to hang the block on (edit mode: no `pattern:` line)."""


def edit_yaml(path, table, new_block, dry_run, allow_time_column_anchor=False):
    """Insert or replace the table's observedCoverage block; returns 'UPDATED', 'UNCHANGED' or 'DRY-RUN'.

    The block goes immediately after the table's `pattern:` line. A table with no `pattern:`
    (an unpartitioned table) is anchored on its `timeColumn:` line when allow_time_column_anchor
    is set, otherwise NoAnchor is raised.
    """
    with open(path, "r", encoding="utf-8") as f:
        lines = f.read().split("\n")

    name_re = re.compile(r"^  - name: " + re.escape(table) + r"\s*$")
    next_item_re = re.compile(r"^  - name: ")
    pattern_re = re.compile(r"^    pattern:\s")
    time_re = re.compile(r"^    timeColumn:\s")

    start = None
    for i, line in enumerate(lines):
        if name_re.match(line):
            start = i
            break
    if start is None:
        raise KeyError(f"no '- name: {table}' entry found in {path}")

    # Bound the search for this table's own anchor line at the next sibling entry.
    end = len(lines)
    for i in range(start + 1, len(lines)):
        if next_item_re.match(lines[i]):
            end = i
            break

    anchor_idx = None
    for i in range(start + 1, end):
        if pattern_re.match(lines[i]):
            anchor_idx = i
            break
    if anchor_idx is None and allow_time_column_anchor:
        for i in range(start + 1, end):
            if time_re.match(lines[i]):
                anchor_idx = i
                break
    if anchor_idx is None:
        raise NoAnchor(table)

    # Remove an existing observedCoverage block immediately after the anchor, if present --
    # identified by our own marker so we never touch a differently-purposed key.
    remove_end = anchor_idx + 1
    if remove_end < len(lines) and MARKER in lines[remove_end]:
        j = remove_end + 1
        while j < len(lines) and (lines[j].strip() == "" or indent_of(lines[j]) > 4):
            # A blank line immediately inside our own block never occurs (we don't emit
            # one), so stop at the first blank line too rather than eating unrelated
            # trailing blank lines that separate this table from the next.
            if lines[j].strip() == "":
                break
            j += 1
        old_block = lines[remove_end:j]
        if old_block == new_block:
            return "UNCHANGED"
        lines[remove_end:j] = []

    lines[anchor_idx + 1:anchor_idx + 1] = new_block

    if dry_run:
        print(f"DRY RUN — would update {table} in {path}:")
        print("\n".join(new_block))
        return "DRY-RUN"

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return "UPDATED"


# ── Measure mode ─────────────────────────────────────────────────────────────────────────────

INTEGER_TYPES = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT",
                 "UINTEGER", "UBIGINT", "UHUGEINT"}
DATE_TYPES = {"DATE", "TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS",
              "TIMESTAMP WITH TIME ZONE"}
KIND_INTEGER, KIND_DATE, KIND_LABEL = "integer", "date", "label"

_YEAR = re.compile(r"^(\d{4})$")
_ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?:[T ].*)?$")
_SPAN4 = re.compile(r"^(\d{4})-(\d{4})$")
_SPAN2 = re.compile(r"^(\d{4})-(\d{2})$")


class MeasureError(Exception):
    """A value or column that the documented rule cannot turn into a year."""


def column_kind(duck_type):
    t = duck_type.upper()
    if t in INTEGER_TYPES:
        return KIND_INTEGER
    if t in DATE_TYPES:
        return KIND_DATE
    if t == "VARCHAR":
        return KIND_LABEL
    raise MeasureError(f"unsupported time column type {duck_type}: need an integer, DATE, "
                       f"TIMESTAMP or VARCHAR column")


_COMPACT = re.compile(r"^(\d{4})(\d{2})(\d{2})$")
FAMILY_LABEL, FAMILY_MONTH = "label", "month"


def _is_label(start, yy):
    return yy == (start + 1) % 100


def _is_month(yy):
    return 1 <= yy <= 12


def two_digit_family(values):
    """Whether the YYYY-NN values of a column are school-year labels or year-months.

    Only values valid as exactly one reading vote; a column with no votes (nothing of that shape,
    or only values valid both ways) has no family and None is returned. Both readings voted for
    raises, as does a YYYY-NN value valid as neither (reported by parse_year_label).
    """
    labels = months = 0
    for v in values:
        m = _SPAN2.match(v)
        if not m:
            continue
        start, yy = int(m.group(1)), int(m.group(2))
        lab, mon = _is_label(start, yy), _is_month(yy)
        if lab and not mon:
            labels += 1
        elif mon and not lab:
            months += 1
    if labels and months:
        raise MeasureError("column mixes YYYY-YY labels and YYYY-MM year-months")
    if labels:
        return FAMILY_LABEL
    if months:
        return FAMILY_MONTH
    return None


def parse_year_label(value, family=None):
    """The year a VARCHAR time value stands for, per the header's rule. Raises MeasureError."""
    m = _YEAR.match(value)
    if m:
        return int(m.group(1))
    m = _ISO.match(value) or _COMPACT.match(value)
    if m:
        try:
            datetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            raise MeasureError(f"unparseable time value {value!r}: not a real calendar date")
        return int(m.group(1))
    m = _SPAN4.match(value)
    if m:
        start, end = int(m.group(1)), int(m.group(2))
        if end != start + 1:
            raise MeasureError(f"unparseable time value {value!r}: span label must end the "
                               f"year after it starts")
        return end
    m = _SPAN2.match(value)
    if m:
        start, yy = int(m.group(1)), int(m.group(2))
        if family is None:
            raise MeasureError(f"ambiguous time value {value!r}: a YYYY-NN value valid as both "
                               f"a school-year label and a year-month, and no other value in the "
                               f"column says which")
        if family == FAMILY_LABEL and _is_label(start, yy):
            return start + 1
        if family == FAMILY_MONTH and _is_month(yy):
            return start
        raise MeasureError(f"unparseable time value {value!r}: not a valid {family} for this column")
    raise MeasureError(f"unparseable time value {value!r}")


MIN_PLAUSIBLE_YEAR = 1000
FUTURE_YEARS_ALLOWED = 10


WINDOW_KEYS = {"minYear", "maxFutureYears", "reason"}


def validate_window(window):
    """A table's declared coverageWindow, checked; returns (minYear, maxFutureYears)."""
    if not isinstance(window, dict):
        raise MeasureError(f"coverageWindow must be a mapping, got {window!r}")
    unknown = set(window) - WINDOW_KEYS
    missing = WINDOW_KEYS - set(window)
    if unknown or missing:
        raise MeasureError(f"coverageWindow needs exactly {sorted(WINDOW_KEYS)}; "
                           f"unknown {sorted(unknown)}, missing {sorted(missing)}")
    lo, future, reason = window["minYear"], window["maxFutureYears"], window["reason"]
    if isinstance(lo, bool) or not isinstance(lo, int) or lo < 1:
        raise MeasureError(f"coverageWindow.minYear must be a positive integer, got {lo!r}")
    if isinstance(future, bool) or not isinstance(future, int) or future < 0:
        raise MeasureError(f"coverageWindow.maxFutureYears must be an integer >= 0, got {future!r}")
    if not isinstance(reason, str) or not reason.strip():
        raise MeasureError("coverageWindow.reason must say why the window is declared")
    return lo, future


def summarize_years(year_counts, row_count, max_year, window_min=None):
    """Block fields from {year: rows}. year_counts is empty only when no row had a time value.

    Without a declared window (window_min None) a year outside MIN_PLAUSIBLE_YEAR..max_year fails
    the measurement. With one, rows outside window_min..max_year are counted into 'excluded' and
    left out of the year fields.
    """
    years = sorted(year_counts)
    if not years:
        raise MeasureError(f"table has {row_count} rows but none has a time value")
    excluded = None
    if window_min is None:
        bad = [y for y in years if not MIN_PLAUSIBLE_YEAR <= y <= max_year]
        if bad:
            raise MeasureError(f"implausible year(s) {bad[:5]} (allowed {MIN_PLAUSIBLE_YEAR}.."
                               f"{max_year}): source data error, not measured")
    else:
        out = [y for y in years if not window_min <= y <= max_year]
        excluded = {"rows": sum(year_counts[y] for y in out), "min_year": window_min,
                    "max_year": max_year}
        years = [y for y in years if window_min <= y <= max_year]
        if not years:
            raise MeasureError(f"coverageWindow {window_min}..{max_year} leaves no row to measure")
    return {
        "min_year": years[0],
        "max_year": years[-1],
        "distinct_years": len(years),
        "contiguous": years[-1] - years[0] + 1 == len(years),
        "years": years,
        "row_count": row_count,
        "excluded": excluded,
    }


def run_duckdb(setup_sql, sql):
    """Run SQL in the duckdb CLI; returns parsed CSV rows (list of dicts). Raises on error."""
    proc = subprocess.run(["duckdb", "-csv", "-c", setup_sql + "\n" + sql],
                          capture_output=True, text=True)
    if proc.returncode != 0:
        raise MeasureError("duckdb failed: " + (proc.stderr.strip() or proc.stdout.strip()))
    return list(csv.DictReader(io.StringIO(proc.stdout)))


def measure_relation(setup_sql, relation, column, max_year, runner=run_duckdb, window_min=None):
    """Measure one time column of a FROM-able relation (an Iceberg scan, or a test table).

    Returns None for a readable table with no rows, else the dict from summarize_years.
    """
    desc = runner(setup_sql, f"DESCRIBE SELECT {column} FROM {relation};")
    if not desc:
        raise MeasureError(f"column {column} not found")
    duck_type = desc[0]["column_type"]
    kind = column_kind(duck_type)
    row_count = int(runner(setup_sql, f"SELECT count(*) AS n FROM {relation};")[0]["n"])
    if row_count == 0:
        return None
    if kind == KIND_INTEGER:
        expr = f"CAST({column} AS BIGINT)"
    elif kind == KIND_DATE:
        expr = (f"year(timezone('UTC', {column}))" if "TIME ZONE" in duck_type.upper()
                else f"year({column})")
    else:
        expr = f"CAST({column} AS VARCHAR)"
    rows = runner(setup_sql,
                  f"SELECT {expr} AS v, count(*) AS n FROM {relation} "
                  f"WHERE {column} IS NOT NULL GROUP BY 1;")
    year_counts = {}
    family = two_digit_family([r["v"] for r in rows]) if kind == KIND_LABEL else None
    for r in rows:
        if kind == KIND_LABEL:
            y = parse_year_label(r["v"], family)
        else:
            y = int(r["v"])
        year_counts[y] = year_counts.get(y, 0) + int(r["n"])
    return summarize_years(year_counts, row_count, max_year, window_min)


def block_from_measurement(m, checked_at):
    if m is None:
        return build_block(None, None, None, None, None, 0, checked_at, empty=True)
    return build_block(m["min_year"], m["max_year"], m["distinct_years"], m["contiguous"],
                       m["years"], m["row_count"], checked_at, excluded=m["excluded"])


def schema_name_default(value):
    m = re.match(r"^\$\{[A-Z_]+:([a-zA-Z_]+)\}$", value) or re.match(r"^([a-zA-Z_]+)$", value)
    if not m:
        raise MeasureError(f"cannot resolve schemaName {value!r}")
    return m.group(1)


def time_column_tables(resources_dir):
    """[(yaml_path, schema, table, timeColumn, coverageWindow-or-None)] per timeColumn table."""
    import yaml  # PyYAML; read-only here, the YAML itself is edited as text
    out = []
    for path in sorted(glob.glob(os.path.join(resources_dir, "*", "*-schema.yaml"))):
        with open(path, "r", encoding="utf-8") as f:
            doc = yaml.safe_load(f)
        schema = schema_name_default(doc["schemaName"])
        for key in ("tables", "partitionedTables"):
            for t in doc.get(key) or []:
                if t.get("timeColumn"):
                    out.append((path, schema, t["name"], t["timeColumn"], t.get("coverageWindow")))
    return out


def local_table_path(mirror_dir, schema, table):
    """The mirrored Iceberg table directory: flat <schema>/<table>, else <schema>/<schema>/<table>."""
    for parts in ((schema, table), (schema, schema, table)):
        d = os.path.join(mirror_dir, *parts)
        if os.path.isdir(os.path.join(d, "metadata")):
            return d
    return None


def measure_mode(args):
    wanted = set(t for t in args.tables.split(",") if t) if args.tables else None
    failures = 0
    checked_year = int(args.checked_at[:4])
    for path, schema, table, column, window in time_column_tables(args.resources_dir):
        if wanted is not None and table not in wanted and f"{schema}.{table}" not in wanted:
            continue
        local = local_table_path(args.mirror_dir, schema, table)
        if local is None:
            print(f"NOT-MIRRORED {schema}.{table}")
            continue
        relation = f"iceberg_scan('{local}', allow_moved_paths=true)"
        setup = "INSTALL iceberg; LOAD iceberg; SET unsafe_enable_version_guessing = true;"
        try:
            if window is None:
                max_year, window_min = checked_year + FUTURE_YEARS_ALLOWED, None
            else:
                window_min, future = validate_window(window)
                max_year = checked_year + future
            m = measure_relation(setup, relation, column, max_year, window_min=window_min)
            block = block_from_measurement(m, args.checked_at)
            status = edit_yaml(path, table, block, args.dry_run, allow_time_column_anchor=True)
        except (MeasureError, NoAnchor, KeyError) as e:
            print(f"FAILED {schema}.{table} ({column}): {e!r}")
            failures += 1
            continue
        if m is None:
            print(f"{status} {schema}.{table} ({column}): 0 rows")
        else:
            print(f"{status} {schema}.{table} ({column}): {m['min_year']}-{m['max_year']} "
                  f"distinct={m['distinct_years']} contiguous={m['contiguous']} "
                  f"rows={m['row_count']}"
                  + (f" excluded={m['excluded']['rows']}" if m["excluded"] else ""))
    return 1 if failures else 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--measure", action="store_true",
                   help="measure mode: measure every timeColumn table found under --mirror-dir")
    p.add_argument("--mirror-dir", help="measure mode: directory holding the mc-mirrored tables")
    p.add_argument("--resources-dir", help="measure mode: govdata src/main/resources")
    p.add_argument("--tables", default="", help="measure mode: comma list of table or schema.table")
    p.add_argument("--file")
    p.add_argument("--table")
    p.add_argument("--min-year", type=int)
    p.add_argument("--max-year", type=int)
    p.add_argument("--distinct-years", type=int)
    p.add_argument("--contiguous", choices=["true", "false"])
    p.add_argument("--years", default="", help="comma-separated explicit year list, required when --contiguous=false")
    p.add_argument("--row-count", type=int)
    p.add_argument("--checked-at", required=True)
    p.add_argument("--empty", action="store_true",
                   help="table is readable but holds no rows; emit rowCount/checkedAt only")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if args.measure:
        if not (args.mirror_dir and args.resources_dir):
            p.error("--measure requires --mirror-dir and --resources-dir")
        sys.exit(measure_mode(args))

    if not (args.file and args.table and args.row_count is not None):
        p.error("edit mode requires --file, --table and --row-count")

    if not args.empty:
        missing = [n for n, v in (("--min-year", args.min_year), ("--max-year", args.max_year),
                                  ("--distinct-years", args.distinct_years),
                                  ("--contiguous", args.contiguous)) if v is None]
        if missing:
            p.error("missing required argument(s) without --empty: " + ", ".join(missing))

    years = [int(y) for y in args.years.split(",") if y.strip()] if args.years else []
    new_block = build_block(
        args.min_year, args.max_year, args.distinct_years,
        args.contiguous == "true", years, args.row_count, args.checked_at,
        empty=args.empty,
    )
    try:
        status = edit_yaml(args.file, args.table, new_block, args.dry_run)
    except KeyError as e:
        print(f"ERROR: {e.args[0]}", file=sys.stderr)
        sys.exit(1)
    except NoAnchor:
        print(f"SKIP: {args.table} has no 'pattern:' line — no year partition to report on", file=sys.stderr)
        sys.exit(3)
    if status == "UNCHANGED":
        print(f"UNCHANGED: {args.table}")
        sys.exit(0)
    if status == "DRY-RUN":
        sys.exit(0)
    print(f"UPDATED: {args.table}")


if __name__ == "__main__":
    main()
