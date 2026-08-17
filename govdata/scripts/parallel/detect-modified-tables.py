#!/usr/bin/env python3
"""detect-modified-tables.py — "fix modified tables" auto-detection layer.

Finds tables whose ETL definition (their own schema-YAML block, or a Java class that block
references by fully-qualified name) changed more recently than that table last finished
processing — i.e. real candidates for force-reprocess.sh that nothing has caught up with yet.

Design (converged on across a long session, see govdata/scripts/parallel/force-reprocess.sh's
header for the sibling execution half):

  1. Only look at files git says changed in the window (--since, default 48h ago) — not every
     table in every schema. Cheap, and matches "what actually moved" rather than a full rescan.
  2. For a changed schema YAML, only tables whose OWN block overlaps a diff hunk are candidates
     — not every table in that file. Block boundaries come from a plain text scan (`- name: X`
     at 2-space indent), matched against unified-diff hunk ranges.
  3. A changed Java file is attributed to a table only if exactly ONE table's block references
     its fully-qualified class name. Referenced by zero tables, or by more than one, is reported
     separately as a framework/shared change — never auto-attributed, because a single table's
     own git history can't see an indirect dependency on shared code (file/.../etl/*.java is the
     common case). That needs a human/agent judgment call: reprocess a named set of tables or
     schemas explicitly, the same manual path force-reprocess.sh already supports.
  4. A changed table is only a real candidate if its most recent change (git commit time, via
     `git log -L` for the block — precise line-range history, not a heuristic) is AFTER its
     tracker's last successful as_of. If the table was already reprocessed since the change,
     it's excluded — nothing to do.

Output: for each schema with candidates, a ready-to-run force-reprocess.sh command line. Exit
code is 0 always (this is a report, not an executor) — piping into force-reprocess.sh is a
separate, explicit step so nothing reprocesses without a human/agent looking at the list first.
"""
import argparse
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = subprocess.run(
    ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True
).stdout.strip()

SCHEMA_YAML_GLOB_PREFIX = "govdata/src/main/resources/"
CLASS_RE = re.compile(r"org\.apache\.calcite\.adapter\.(?:govdata|file)\.[A-Za-z0-9_.]+")
TABLE_HEADER_RE = re.compile(r"^(\s*)- name:\s*(\S+)\s*$")
SECTION_KEY_RE = re.compile(r"^(\s*)(tables|views|partitionedTables):\s*$")
HUNK_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")

JAVA_ROOTS = [
    Path(REPO_ROOT) / "govdata" / "src" / "main" / "java",
    Path(REPO_ROOT) / "file" / "src" / "main" / "java",
]

FRAMEWORK_PATH_MARKERS = ("/etl/",)


def sh(args, cwd=REPO_ROOT):
    r = subprocess.run(args, capture_output=True, text=True, cwd=cwd)
    if r.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(args)}\n{r.stderr}")
    return r.stdout


def resolve_since_commit(since):
    return sh(["git", "rev-list", "-1", f"--before={since}", "HEAD"]).strip()


def changed_files(base_commit):
    out = sh(["git", "diff", "--name-only", f"{base_commit}..HEAD"])
    return [l for l in out.splitlines() if l.strip()]


def read_head(path):
    r = subprocess.run(["git", "show", f"HEAD:{path}"], capture_output=True, text=True, cwd=REPO_ROOT)
    return r.stdout if r.returncode == 0 else None


def table_blocks(yaml_text):
    """Yields (table_name, start_line, end_line_exclusive, section) using 1-based line numbers,
    for every '- name: X' list item directly under a top-level tables:/views:/partitionedTables:
    section key. Anchored on the section key's own indent (list items live at indent+2) rather
    than "most common indent across the whole file" — schema YAMLs have far more '- name:'
    entries nested under columns/dimensions than under the table list itself, so a frequency
    heuristic silently matches the wrong nesting level (verified: it picked up dimension-value
    and column names as "tables" on econ/officials/ref before this fix)."""
    lines = yaml_text.splitlines()
    sections = []  # (section_indent, list_item_indent, name, start_line, section_name)
    i = 0
    n = len(lines)
    while i < n:
        m = SECTION_KEY_RE.match(lines[i])
        if not m:
            i += 1
            continue
        section_indent = len(m.group(1))
        section_name = m.group(2)
        item_indent = section_indent + 2
        j = i + 1
        while j < n:
            line = lines[j]
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                cur_indent = len(line) - len(line.lstrip(" "))
                if cur_indent <= section_indent:
                    break
            hm = TABLE_HEADER_RE.match(line)
            if hm and len(hm.group(1)) == item_indent:
                sections.append((item_indent, hm.group(2), j + 1, section_name))
            j += 1
        i = j
    for idx, (_, name, start, section_name) in enumerate(sections):
        end = sections[idx + 1][2] - 1 if idx + 1 < len(sections) else len(lines) + 1
        yield name, start, end, section_name


def diff_hunks_new_ranges(base_commit, path):
    """Returns list of (start, end_exclusive) line ranges in the HEAD version of `path` that a
    diff hunk touched, from `@@ -a,b +c,d @@` headers."""
    out = sh(["git", "diff", "-U0", f"{base_commit}..HEAD", "--", path])
    ranges = []
    for line in out.splitlines():
        m = HUNK_RE.match(line)
        if m:
            c = int(m.group(1))
            d = int(m.group(2)) if m.group(2) else 1
            if d == 0:
                # Pure deletion in the new file — anchor on the line before the deletion point.
                ranges.append((max(c, 1), max(c, 1) + 1))
            else:
                ranges.append((c, c + d))
    return ranges


def overlaps(a_start, a_end, b_start, b_end):
    return a_start < b_end and b_start < a_end


def block_last_changed(path, start, end):
    """Most recent commit ISO timestamp touching this exact line range, via git's own
    line-range history tracking (handles surrounding edits shifting the range correctly)."""
    out = sh(["git", "log", "-1", "--format=%cI", "-L", f"{start},{end}:{path}"])
    ts = out.splitlines()[0].strip() if out.strip() else ""
    return ts or None


def file_last_changed(path):
    out = sh(["git", "log", "-1", "--format=%cI", "--", path]).strip()
    return out or None


def fqcn_to_relpath(fqcn):
    tail = fqcn.replace(".", "/") + ".java"
    for root in JAVA_ROOTS:
        candidate = root / tail
        rel = str(candidate.relative_to(REPO_ROOT))
        if (Path(REPO_ROOT) / rel).exists() or read_head(rel) is not None:
            return rel
    return None


def schema_name_of(yaml_path):
    return Path(yaml_path).name.replace("-schema.yaml", "")


def pg_as_of(table):
    """Queries the tracker for `table`'s most recent successful incremental as_of, in millis
    epoch (0 if none), by shelling into the SAME bash helpers force-reprocess.sh uses — no
    second implementation of the PG connection/namespace logic to drift from the real one."""
    script_dir = Path(REPO_ROOT) / "govdata" / "scripts" / "parallel"
    bash_cmd = f"""
set -euo pipefail
source "{script_dir}/common.sh"
source "{script_dir.parent}/tracker_pg.sh"
load_env
ns=$(pg_ns_from_bucket "${{GOVDATA_PARQUET_DIR:-s3://govdata-parquet-v1}}")
pg_tracker_exec "SELECT COALESCE(MAX(as_of),0) FROM \\"${{ns}}\\".pipeline_tracker WHERE table_name = '{table}' AND phase = 'incremental' AND state = 'complete';"
"""
    r = subprocess.run(["bash", "-c", bash_cmd], capture_output=True, text=True, cwd=REPO_ROOT)
    if r.returncode != 0:
        print(f"  WARNING: could not query tracker as_of for {table}: {r.stderr.strip()}", file=sys.stderr)
        return None
    out = r.stdout.strip()
    try:
        return int(out)
    except ValueError:
        return None


def iso_to_millis(iso_ts):
    import datetime
    return int(datetime.datetime.fromisoformat(iso_ts).timestamp() * 1000)


# run-pool.sh splits SEC into performance-partitioning pool slots (sec_primary: 10-K/10-Q,
# sec_secondary: 8-K/proxy/insider/13D-G, sec_13f: 13F-HR, sec_prices: Stooq) so historical
# backfills parallelize across workers. Those slot names are NOT what force-reprocess.sh should
# target though: worker.sh's bare `sec` case (see worker.sh's "SEC — universal historical|daily
# entry point") passes no filingTypes filter by default, so a single `sec:<range>` run fetches
# every filing type and repopulates every document-derived table — filing_metadata,
# financial_line_items, filing_contexts, mda_sections, xbrl_relationships, insider_transactions,
# institutional_holdings, beneficial_ownership, earnings_transcripts, vectorized_chunks — in one
# pass, which is both simpler and safer than trying to attribute each table to one of the
# parallelization slots (most of those tables are populated from accessions across MULTIPLE form
# types, so a single-slot run would silently under-cover them). The one real exception is
# stock_prices: worker.sh's `sec` case hardcodes fetchStockPrices:false, so stock_prices is only
# ever populated by the dedicated `sec_prices` slot.
SEC_PRICE_ONLY_TABLES = {"stock_prices"}

# Schemas with no point-in-time history to backfill at all (current-snapshot reference data with
# no year axis) — run-pool.sh's `historical` and `all` aliases deliberately never queue a
# :historical slot for these (see run-pool.sh's historical) alias comment), so
# force-reprocess.sh's default historical-range lookup would only ever find a missing marker file
# and error. Skip straight to --skip-historical for them. officials is NOT here: worker.sh's
# officials case now converts GOVDATA_START_YEAR to the covering Congress internally and gets a
# normal (single-pass :once) historical slot, same as every other year-addressed schema.
DAILY_ONLY_SCHEMAS = {"ref", "econ_reference", "cyber_threat"}


def sec_pool_schema_splits(schema, tables):
    """Yields (pool_schema, comma_joined_tables) — for `sec`, splits stock_prices (needs the
    sec_prices pool slot) from everything else (needs bare sec); every other schema is 1:1."""
    if schema != "sec":
        yield schema, ",".join(sorted(set(tables)))
        return
    price_tables = sorted(set(t for t in tables if t in SEC_PRICE_ONLY_TABLES))
    other_tables = sorted(set(t for t in tables if t not in SEC_PRICE_ONLY_TABLES))
    if other_tables:
        yield "sec", ",".join(other_tables)
    if price_tables:
        yield "sec_prices", ",".join(price_tables)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--since", default="48 hours ago",
                     help="git-recognized time expression for the lookback window (default: '48 hours ago')")
    ap.add_argument("--base-commit", default=None,
                     help="use this commit as the baseline instead of resolving --since")
    ap.add_argument("--skip-tracker-check", action="store_true",
                     help="list every changed table as a candidate without querying as_of (faster, less precise)")
    args = ap.parse_args()

    base_commit = args.base_commit or resolve_since_commit(args.since)
    if not base_commit:
        print(f"ERROR: no commit found for --since '{args.since}'", file=sys.stderr)
        sys.exit(2)
    print(f"Baseline commit: {base_commit} ({sh(['git','log','-1','--format=%cI',base_commit]).strip()})")
    print(f"Window: {base_commit}..HEAD\n")

    files = changed_files(base_commit)
    changed_yamls = sorted(f for f in files if f.startswith(SCHEMA_YAML_GLOB_PREFIX) and f.endswith(".yaml"))
    changed_java = sorted(f for f in files if f.endswith(".java")
                           and (f.startswith("govdata/src/main/java/") or f.startswith("file/src/main/java/")))

    if not changed_yamls and not changed_java:
        print("No govdata schema YAML or ETL Java changes in this window. Nothing to do.")
        return

    # ── Step 1: tables whose own YAML block overlaps a diff hunk ──
    direct_candidates = {}  # (schema, table) -> yaml_path
    for yaml_path in changed_yamls:
        head_text = read_head(yaml_path)
        if head_text is None:
            continue  # deleted at HEAD
        hunks = diff_hunks_new_ranges(base_commit, yaml_path)
        if not hunks:
            continue
        schema = schema_name_of(yaml_path)
        for name, start, end, section in table_blocks(head_text):
            if section == "views":
                continue  # SQL views are computed over other tables, never independently ETL'd/tracked
            if any(overlaps(start, end + 1, hs, he) for hs, he in hunks):
                direct_candidates[(schema, name)] = yaml_path

    # ── Step 2: build a class -> referencing (schema, table) map, but only for FQCNs referenced
    #    anywhere, restricted to schemas whose YAML we actually need to scan (all of them, since
    #    a changed Java file could be referenced by a table in any schema — but this is one pass
    #    over already-local text, not per-file git calls). ──
    changed_java_fqcns = set()
    relpath_to_fqcn = {}
    for jf in changed_java:
        for root in JAVA_ROOTS:
            try:
                rel = Path(jf).relative_to(root.relative_to(REPO_ROOT))
            except ValueError:
                continue
            fqcn = str(rel.with_suffix("")).replace("/", ".")
            changed_java_fqcns.add(fqcn)
            relpath_to_fqcn[jf] = fqcn
            break

    class_refs = {}  # fqcn -> list of (schema, table)
    if changed_java_fqcns:
        all_yamls = sorted(str(p.relative_to(REPO_ROOT)) for p in
                            Path(REPO_ROOT, SCHEMA_YAML_GLOB_PREFIX).rglob("*-schema.yaml"))
        for yaml_path in all_yamls:
            head_text = read_head(yaml_path) if yaml_path not in changed_yamls else None
            if head_text is None:
                head_text = Path(REPO_ROOT, yaml_path).read_text()
            schema = schema_name_of(yaml_path)
            for name, start, end, section in table_blocks(head_text):
                if section == "views":
                    continue
                block_text = "\n".join(head_text.splitlines()[start - 1:end - 1])
                for fqcn in CLASS_RE.findall(block_text):
                    if fqcn in changed_java_fqcns:
                        class_refs.setdefault(fqcn, []).append((schema, name))

    framework_changes = []  # (java_path, referencing_tables)
    for jf in changed_java:
        fqcn = relpath_to_fqcn.get(jf)
        refs = sorted(set(class_refs.get(fqcn, []))) if fqcn else []
        is_framework_path = any(m in jf for m in FRAMEWORK_PATH_MARKERS)
        if len(refs) == 1 and not is_framework_path:
            direct_candidates.setdefault(refs[0], None)
        else:
            framework_changes.append((jf, refs))

    if not direct_candidates and not framework_changes:
        print("Changed files found, but none map to a specific table or class reference. Nothing to do.")
        return

    # ── Step 3: for each direct candidate, compute its precise last-changed time and compare
    #    against the tracker's as_of. ──
    print(f"── Direct candidates: {len(direct_candidates)} table(s) touched in this window ──\n")
    results_by_schema = {}
    excluded = []
    for (schema, table), yaml_path in sorted(direct_candidates.items()):
        if yaml_path is None:
            # matched via a Java class reference (Step 2); find its YAML for the block range.
            for yp in Path(REPO_ROOT, SCHEMA_YAML_GLOB_PREFIX).rglob(f"{schema}-schema.yaml"):
                yaml_path = str(yp.relative_to(REPO_ROOT))
                break
        head_text = read_head(yaml_path)
        block = next(((n, s, e) for n, s, e, sec in table_blocks(head_text) if n == table), None)
        last_changed_iso = block_last_changed(yaml_path, block[1], block[2] - 1) if block else None
        last_changed_ms = iso_to_millis(last_changed_iso) if last_changed_iso else 0

        as_of_ms = None if args.skip_tracker_check else pg_as_of(table)
        as_of_ms = as_of_ms or 0

        if args.skip_tracker_check or last_changed_ms > as_of_ms:
            results_by_schema.setdefault(schema, []).append(table)
            status = "no prior successful run found" if as_of_ms == 0 else f"as_of={as_of_ms}"
            print(f"  CANDIDATE  {schema}.{table} — changed {last_changed_iso}, {status}")
        else:
            excluded.append((schema, table, last_changed_iso, as_of_ms))
            print(f"  excluded   {schema}.{table} — changed {last_changed_iso}, already reprocessed (as_of={as_of_ms})")

    if framework_changes:
        print(f"\n── Framework / shared-code changes: {len(framework_changes)} file(s) — NOT auto-attributed ──")
        print("   (touched by 0 or 2+ tables, or under an etl/ path — indirect impact, needs a manual call)\n")
        for jf, refs in framework_changes:
            who = ", ".join(f"{s}.{t}" for s, t in refs) if refs else "no direct table references found"
            print(f"  {jf}")
            print(f"    referenced by: {who}")

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    if results_by_schema:
        for schema, tables in sorted(results_by_schema.items()):
            for pool_schema, tables_arg in sec_pool_schema_splits(schema, tables):
                print(f"\n{schema}:" if pool_schema == schema else f"\n{schema}  (pool schema: {pool_schema}):")
                skip_hist = " --skip-historical" if pool_schema in DAILY_ONLY_SCHEMAS else ""
                print(f"  govdata/scripts/parallel/force-reprocess.sh --schema {pool_schema} --tables {tables_arg}{skip_hist}")
    else:
        print("\nNo tables need reprocessing (all changes already reflected in the tracker, or none found).")
    if framework_changes:
        print(f"\n{len(framework_changes)} shared-code change(s) need manual review — see above. These are NOT")
        print("included in the commands above; decide which tables/schemas they actually affect and run")
        print("force-reprocess.sh against that explicit list.")


if __name__ == "__main__":
    main()
