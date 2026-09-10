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
"""Check whether every table declared in the govdata schema YAMLs is actually
materialized (and reasonably fresh) in the real object store.

Ground truth for "what tables should exist" comes from `partitionedTables:` in each
`<schema>/<name>-schema.yaml` (the same field `dq_coverage_check.py` reads) — never from
memory. For each table this probes the object store directly with `iceberg_snapshots()` /
`iceberg_scan()` (never a raw glob or `read_parquet`) and classifies it:

  MISSING  no Iceberg table found at any known warehouse-path convention
  EMPTY    table found, latest snapshot exists, but COUNT(*) == 0
  STALE    table found and non-empty, but the latest snapshot is older than the
           threshold for that schema's declared update cadence
  OK       fresh and non-empty

Two warehouse-path conventions are tried per table (flat `<schema>/<table>` and
double-nested `<schema>/<schema>/<table>`) because different workers have historically
pre-suffixed their `directory` operand differently (see the data-purge-health-path-mismatch
memory) — this script does not assume which applies to a given schema, it probes both and
uses whichever responds.

This is an existence/freshness monitor, not a data-quality tool: it does not check row
content, key uniqueness, or value ranges (that's `run-all-dq.sh` / `model-verify.sh`'s job
respectively). It prints a console report only — it never writes to the Defect Register or
any other tracking artifact.

Two modes:

  (default)  One-shot full sweep — probes every expected table right now. Cheap (~1 min
             for the whole fleet) but puts a burst of ~2x table-count requests on MinIO
             all at once.

  --tick     Queue mode for continuous, low-load monitoring. Loads a small persisted state
             file (one entry per expected table: last known status + when it's next due),
             probes only the tables that are actually due (capped at --batch-size), updates
             their due time by a recheck interval derived from that table's schema cadence
             (so daily-cadence tables get rechecked far more often than annual ones without
             any extra bookkeeping), and reports both this tick's changes and the full
             fleet's cached status. Intended to be fired on a loose recurring schedule (the
             `loop` skill, or a durable cron job) rather than run to completion in one shot
             — see the etl-materialization-check skill for the recommended cadence.
"""

import argparse
import glob
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

try:
    import yaml
except ImportError:
    sys.stderr.write("PyYAML required: pip install pyyaml\n")
    sys.exit(2)

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # .../govdata
RESOURCES = os.path.join(REPO, "src", "main", "resources")

# Staleness threshold in days, keyed by the update cadence documented in the
# govdata-schema-catalog skill (itself sourced from each schema's actual publication
# pattern). This is SCHEMA-level granularity, not per-table: govdata-data-cadence
# establishes that true per-table freshness is controlled by dimension design (year/month
# variables, dataLag, releaseMonth), not a single declarative field, so there is no cheap
# per-table cadence to read. The schema-level cadence is the coarsest reliable signal
# available without hand-auditing every table's dimension block, and every threshold below
# is deliberately generous (multiple cycles of slack) so a normal publication delay does
# not read as a false alarm. `None` means "existence/non-empty only" — a Rare-cadence table
# being months or years old is expected, not a defect.
CADENCE_DAYS = {
    # Daily
    "sec": 4, "cyber_vuln": 4, "weather": 4, "fedregister": 4,
    # Weekly
    "cyber_threat": 14,
    # Monthly
    "econ": 45, "energy": 45, "environment": 45, "health": 45, "housing": 45,
    # Annual
    "census": 400, "geo": 400, "crime": 400, "lands": 400, "ag": 400,
    "edu": 400, "research": 400,
    # Rolling (irregular/continuous upstream cadence) — moderate default
    "cftc": 30, "disasters": 30, "fec": 30, "patents": 30, "transport": 30,
    "officials": 30, "fiscal": 30, "banking": 30,
    # Rare — no meaningful staleness window
    "ref": None, "econ_reference": None,
}

MISSING_SENTINEL = "No version was provided and no version-hint could be found"

# Queue-mode recheck interval, derived from the same CADENCE_DAYS thresholds: check a
# table often enough to catch it crossing into STALE well before the next full interval,
# without re-probing it needlessly often. A quarter of the staleness threshold gives ~4
# chances to catch a table going stale within one threshold window. Floor and ceiling keep
# both ends sane: a 1-day floor avoids hammering even the tightest (Daily, 4d threshold)
# schemas, and a 60-day ceiling means even Rare-cadence tables (no staleness threshold at
# all — existence/emptiness only) still get re-verified a handful of times a year rather
# than being queued once and forgotten.
DEFAULT_RARE_INTERVAL_DAYS = 30.0
MIN_RECHECK_INTERVAL_DAYS = 1.0
MAX_RECHECK_INTERVAL_DAYS = 60.0


def recheck_interval_days(cadence_days):
    if cadence_days is None:
        return DEFAULT_RARE_INTERVAL_DAYS
    return min(MAX_RECHECK_INTERVAL_DAYS, max(MIN_RECHECK_INTERVAL_DAYS, cadence_days / 4.0))


def schema_key(yaml_path):
    base = os.path.basename(yaml_path)
    for suffix in ("-schema.yaml", ".yaml"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    return base.replace("-", "_")


def load_expected_tables(only):
    """Returns {schema_key: [table_name, ...]}, skipping smoke/test fixtures."""
    expected = {}
    for path in sorted(glob.glob(os.path.join(RESOURCES, "*", "*.yaml"))):
        try:
            doc = yaml.safe_load(open(path))
        except Exception:
            continue
        if not isinstance(doc, dict) or not doc.get("partitionedTables"):
            continue
        key = schema_key(path)
        if "smoke" in key or key.endswith("_test"):
            continue
        if only and key not in only:
            continue
        tables = sorted(
            t["name"] for t in doc["partitionedTables"]
            if isinstance(t, dict) and t.get("name")
        )
        if tables:
            expected[key] = tables
    return expected


def s3_setup_sql(env):
    endpoint = env["AWS_ENDPOINT_OVERRIDE"]
    use_ssl = "true" if endpoint.startswith("https://") else "false"
    endpoint = endpoint.split("://", 1)[-1].rstrip("/")
    return "\n".join([
        "INSTALL iceberg; LOAD iceberg;",
        "INSTALL httpfs; LOAD httpfs;",
        "SET s3_access_key_id='%s';" % env["AWS_ACCESS_KEY_ID"],
        "SET s3_secret_access_key='%s';" % env["AWS_SECRET_ACCESS_KEY"],
        "SET s3_endpoint='%s';" % endpoint,
        "SET s3_use_ssl=%s;" % use_ssl,
        "SET s3_url_style='path';",
    ])


def probe_path(setup_sql, path, timeout):
    sql = setup_sql + "\n" + (
        "SELECT "
        "(SELECT MAX(timestamp_ms) FROM iceberg_snapshots('%s')) AS latest_snapshot, "
        "(SELECT COUNT(*) FROM iceberg_scan('%s', allow_moved_paths := true)) AS row_count;"
        % (path, path)
    )
    try:
        proc = subprocess.run(
            ["duckdb", "-json", "-c", sql],
            capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return {"status": "error", "detail": "duckdb probe timed out after %ds" % timeout}
    if proc.returncode == 0 and proc.stdout.strip():
        row = json.loads(proc.stdout)[0]
        if row.get("latest_snapshot") is None:
            return {"status": "missing"}
        return {
            "status": "found", "path": path,
            "latest_snapshot": row["latest_snapshot"],
            "row_count": int(row["row_count"]),
        }
    stderr = proc.stderr.strip()
    if MISSING_SENTINEL in stderr:
        return {"status": "missing"}
    return {"status": "error", "detail": stderr.splitlines()[0] if stderr else "unknown duckdb error"}


def probe_table(bucket, schema, table, setup_sql, timeout):
    candidates = [
        "s3://%s/%s/%s" % (bucket, schema, table),
        "s3://%s/%s/%s/%s" % (bucket, schema, schema, table),
    ]
    last_error = None
    for path in candidates:
        result = probe_path(setup_sql, path, timeout)
        if result["status"] == "found":
            return result
        if result["status"] == "error":
            last_error = result
    if last_error is not None:
        return last_error
    return {"status": "missing"}


def classify(schema, table, probe, cadence_days):
    if probe["status"] == "error":
        return "ERROR", probe["detail"]
    if probe["status"] == "missing":
        return "MISSING", "no Iceberg table at any known warehouse path"
    row_count = probe["row_count"]
    if row_count == 0:
        return "EMPTY", "snapshot exists, 0 rows (path: %s)" % probe["path"]
    latest = datetime.fromisoformat(probe["latest_snapshot"])
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)
    age_days = (datetime.now(timezone.utc) - latest).total_seconds() / 86400.0
    if cadence_days is not None and age_days > cadence_days:
        return "STALE", "latest snapshot %.1fd old (threshold %dd), %d rows" % (
            age_days, cadence_days, row_count)
    return "OK", "%.1fd old, %d rows" % (age_days, row_count)


def default_state_path():
    project_dir = os.environ.get("CLAUDE_PROJECT_DIR")
    if not project_dir:
        project_dir = os.path.dirname(REPO)  # .../govdata/.. -> repo root
    return os.path.join(project_dir, ".claude", "etl-materialization-queue-state.json")


def load_state(path):
    if not os.path.exists(path):
        return {"version": 1, "tables": {}}
    try:
        with open(path) as f:
            state = json.load(f)
    except Exception as e:
        sys.stderr.write("WARNING: could not read state file %s (%s) — starting fresh.\n" % (path, e))
        return {"version": 1, "tables": {}}
    state.setdefault("tables", {})
    return state


def save_state(path, state):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    os.replace(tmp_path, path)


def resolve_env():
    for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_ENDPOINT_OVERRIDE"):
        if not os.environ.get(var):
            sys.stderr.write(
                "ERROR: %s is not set. Source govdata/.env.prod with all three AWS_* vars "
                "exported (`set -a; source govdata/.env.prod; set +a`) before running this "
                "— otherwise the check may silently fall back to a stale endpoint.\n" % var
            )
            return None
    return {k: os.environ[k] for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_ENDPOINT_OVERRIDE")}


def run_probe_batch(bucket, jobs, setup_sql, timeout, max_workers):
    """jobs: list of (schema, table). Returns {(schema, table): {"status", "detail", "probe"}}."""
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(probe_table, bucket, schema, table, setup_sql, timeout): (schema, table)
            for schema, table in jobs
        }
        for fut in as_completed(futures):
            schema, table = futures[fut]
            probe = fut.result()
            cadence_days = CADENCE_DAYS.get(schema)
            status, detail = classify(schema, table, probe, cadence_days)
            results[(schema, table)] = {"status": status, "detail": detail, "probe": probe}
    return results


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", help="comma-separated schema keys to check (default: all)")
    ap.add_argument("--bucket", default=os.environ.get("GOVDATA_PARQUET_BUCKET", "govdata-parquet-v1"))
    ap.add_argument("--jobs", type=int, default=8, help="concurrent duckdb probes (default 8)")
    ap.add_argument("--timeout", type=int, default=60, help="per-probe timeout in seconds")
    ap.add_argument("--json", action="store_true", help="emit machine-readable JSON instead of the console report")
    ap.add_argument("--tick", action="store_true",
                     help="queue mode: probe only the tables currently due (see --batch-size), "
                          "update the persisted state file, and exit — do not sweep the whole fleet")
    ap.add_argument("--batch-size", type=int, default=3,
                     help="[--tick only] max tables to probe this tick (default 3)")
    ap.add_argument("--state-file", default=default_state_path(),
                     help="[--tick only] path to the persisted queue state "
                          "(default: $CLAUDE_PROJECT_DIR/.claude/etl-materialization-queue-state.json)")
    args = ap.parse_args()

    env = resolve_env()
    if env is None:
        return 2
    setup_sql = s3_setup_sql(env)

    only = set(args.source.split(",")) if args.source else None
    expected = load_expected_tables(only)
    if only:
        missing_schemas = only - set(expected)
        if missing_schemas:
            sys.stderr.write("WARNING: no schema YAML found for: %s\n" % ", ".join(sorted(missing_schemas)))
    if not expected:
        sys.stderr.write("ERROR: no expected tables resolved — check --source and resource paths.\n")
        return 2

    for schema in expected:
        if schema not in CADENCE_DAYS:
            sys.stderr.write(
                "WARNING: schema '%s' has no entry in CADENCE_DAYS — add one (see "
                "govdata-schema-catalog for its update cadence) rather than silently "
                "defaulting; skipping freshness classification for its tables.\n" % schema
            )

    if args.tick:
        return run_tick(args, expected, args.bucket, setup_sql)
    return run_full_sweep(args, expected, args.bucket, setup_sql)


def run_full_sweep(args, expected, bucket, setup_sql):
    jobs = [(schema, table) for schema, tables in expected.items() for table in tables]
    results = run_probe_batch(bucket, jobs, setup_sql, args.timeout, args.jobs)

    if args.json:
        out = [
            {"schema": s, "table": t, "status": r["status"], "detail": r["detail"]}
            for (s, t), r in sorted(results.items())
        ]
        print(json.dumps(out, indent=2))
    else:
        print_report(expected, results, CADENCE_DAYS)

    bad = [r for r in results.values() if r["status"] != "OK"]
    return 1 if bad else 0


def run_tick(args, expected, bucket, setup_sql):
    now = datetime.now(timezone.utc)
    state = load_state(args.state_file)
    tables = state["tables"]

    universe = set()
    for schema, table_names in expected.items():
        cadence_days = CADENCE_DAYS.get(schema)
        interval = recheck_interval_days(cadence_days)
        for table in table_names:
            key = "%s.%s" % (schema, table)
            universe.add(key)
            entry = tables.get(key)
            if entry is None:
                # New table (or first run ever) — due immediately, most-urgent-cadence first.
                tables[key] = {
                    "schema": schema, "table": table,
                    "recheck_interval_days": interval,
                    "due_at": now.isoformat(),
                    "status": None, "detail": "never checked", "checked_at": None,
                }
            else:
                entry["recheck_interval_days"] = interval  # cadence may have been retuned

    # Only reconcile within the schemas actually loaded this run — a scoped `--source`
    # tick must never delete other schemas' tracked state just because they're out of
    # scope for this invocation.
    scoped_schemas = set(expected)
    stale_keys = [
        k for k, entry in tables.items()
        if entry["schema"] in scoped_schemas and k not in universe
    ]
    for k in stale_keys:
        del tables[k]  # table removed from its schema YAML since the last tick

    def due_at_of(key):
        return datetime.fromisoformat(tables[key]["due_at"])

    due_now = sorted(
        (k for k in tables if due_at_of(k) <= now),
        key=lambda k: (due_at_of(k), tables[k]["recheck_interval_days"]),
    )
    batch = due_now[: args.batch_size]

    jobs = [(tables[k]["schema"], tables[k]["table"]) for k in batch]
    results = run_probe_batch(bucket, jobs, setup_sql, args.timeout, min(args.jobs, max(1, len(jobs))))

    transitions = []
    for k in batch:
        schema, table = tables[k]["schema"], tables[k]["table"]
        r = results[(schema, table)]
        old_status = tables[k]["status"]
        if old_status is not None and old_status != r["status"]:
            transitions.append((k, old_status, r["status"], r["detail"]))
        tables[k]["status"] = r["status"]
        tables[k]["detail"] = r["detail"]
        tables[k]["checked_at"] = now.isoformat()
        tables[k]["due_at"] = (now + timedelta(days=tables[k]["recheck_interval_days"])).isoformat()

    save_state(args.state_file, state)

    if args.json:
        print(json.dumps({
            "checked_this_tick": [
                {"schema": tables[k]["schema"], "table": tables[k]["table"],
                 "status": tables[k]["status"], "detail": tables[k]["detail"]}
                for k in batch
            ],
            "transitions": [
                {"table": k, "from": old, "to": new, "detail": detail}
                for k, old, new, detail in transitions
            ],
            "queue_depth_due": len(due_now),
            "fleet_summary": fleet_summary(tables),
        }, indent=2))
    else:
        print_tick_report(tables, batch, due_now, transitions, now)

    bad = [t for t in tables.values() if t["status"] not in (None, "OK")]
    return 1 if bad else 0


def fleet_summary(tables):
    totals = {"OK": 0, "STALE": 0, "EMPTY": 0, "MISSING": 0, "ERROR": 0, "unchecked": 0}
    for t in tables.values():
        totals[t["status"] or "unchecked"] += 1
    return totals


def print_tick_report(tables, batch, due_now, transitions, now):
    print("=" * 78)
    print("  ETL Materialization Check — tick @ %s" % now.strftime("%Y-%m-%d %H:%M UTC"))
    print("=" * 78)
    print("Queue: %d tables tracked, %d due now, %d probed this tick"
          % (len(tables), len(due_now), len(batch)))

    if batch:
        print("\n--- probed this tick ---")
        for k in batch:
            t = tables[k]
            print("  %-8s %-40s %s" % (t["status"], k, t["detail"]))
    else:
        print("\n(nothing due this tick)")

    if transitions:
        print("\n--- STATUS CHANGES ---")
        for k, old, new, detail in transitions:
            flag = "  " if new == "OK" else "**"
            print(" %s %-40s %s -> %s (%s)" % (flag, k, old, new, detail))

    totals = fleet_summary(tables)
    print("\n--- fleet status (cached, as of each table's own last check) ---")
    print("OK=%d STALE=%d EMPTY=%d MISSING=%d ERROR=%d unchecked=%d (tracked=%d)" % (
        totals["OK"], totals["STALE"], totals["EMPTY"], totals["MISSING"], totals["ERROR"],
        totals["unchecked"], len(tables)))
    problems = [(k, t) for k, t in sorted(tables.items()) if t["status"] not in (None, "OK")]
    if problems:
        print("\nCurrently non-OK (may be from an earlier tick, not necessarily this one):")
        for k, t in problems:
            checked = t["checked_at"] or "never"
            print("  %-8s %-40s checked %s — %s" % (t["status"], k, checked, t["detail"]))
    print("=" * 78)


def print_report(expected, results, cadence_days):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print("=" * 78)
    print("  ETL Materialization Check — %s" % now)
    print("=" * 78)

    totals = {"OK": 0, "STALE": 0, "EMPTY": 0, "MISSING": 0, "ERROR": 0}
    for schema in sorted(expected):
        rows = [(t, results[(schema, t)]) for t in expected[schema]]
        bad_rows = [(t, r) for t, r in rows if r["status"] != "OK"]
        threshold = cadence_days.get(schema)
        threshold_str = ("%dd" % threshold) if threshold is not None else "n/a (rare cadence)"
        print("\n--- %s (%d tables, staleness threshold %s) ---" % (schema, len(rows), threshold_str))
        if not bad_rows:
            print("  OK — all tables present, non-empty, within threshold")
        for t, r in bad_rows:
            print("  %-8s %-30s %s" % (r["status"], t, r["detail"]))
        for _, r in rows:
            totals[r["status"]] += 1

    print("\n" + "=" * 78)
    print("SUMMARY: OK=%d STALE=%d EMPTY=%d MISSING=%d ERROR=%d (total=%d)" % (
        totals["OK"], totals["STALE"], totals["EMPTY"], totals["MISSING"], totals["ERROR"],
        sum(totals.values())))
    print("=" * 78)


if __name__ == "__main__":
    sys.exit(main())
