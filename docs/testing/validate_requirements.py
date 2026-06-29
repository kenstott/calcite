#!/usr/bin/env python3
"""Validate the per-adapter requirement ledgers under docs/requirements/*.yaml.

Schema + cross-field checks, plus coverage and a 3-state matrix view. Scoped to the four adapters we
own (file, govdata, splunk, sharepoint-list) — Calcite core is never touched.

Usage (run from repo root, needs python3 + pyyaml):
  python3 docs/testing/validate_requirements.py            # schema validation (exit 1 on error)
  python3 docs/testing/validate_requirements.py --coverage # + list MUST reqs lacking a green test
  python3 docs/testing/validate_requirements.py --matrix   # + per-group status matrix
  python3 docs/testing/validate_requirements.py --orphan   # + tests under */src/test not cited by any req
"""
from __future__ import annotations

import argparse
import glob
import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    sys.exit("PyYAML required: pip install pyyaml (or run under WSL where it's available)")

ROOT = Path(__file__).resolve().parents[2]
LEDGER_DIR = ROOT / "docs" / "requirements"

PREFIX = {"file": "FILE", "govdata": "GOV", "splunk": "SPLUNK", "sharepoint-list": "SPLIST"}
STATUS = {"proposed", "accepted", "in-progress", "complete", "rejected"}
PRIORITY = {"MUST", "SHOULD", "MAY"}
RTYPE = {"behavioral", "structural", "constraint", "ui", "infrastructure"}
MODE = {"read", "write"}
CARD = {"1:1", "1:N"}
TOPO = {"single", "walking"}
SEAM = {"bytes", "catalog", "json-set", "table", "execution"}
REQUIRED = ("id", "module", "status", "group", "priority", "type", "description")
ID_RE = re.compile(r"^[A-Z]+-\d+$")


def load() -> list[dict]:
    reqs = []
    for f in sorted(glob.glob(str(LEDGER_DIR / "*.yaml"))):
        data = yaml.safe_load(Path(f).read_text(encoding="utf-8")) or []
        for r in data:
            r["_file"] = Path(f).name
        reqs.extend(data)
    return reqs


def validate(reqs: list[dict]) -> list[str]:
    errors, seen = [], {}
    for r in reqs:
        rid = r.get("id", "<no-id>")
        for k in REQUIRED:
            if not r.get(k):
                errors.append(f"{rid}: missing required field '{k}'")
        if rid in seen:
            errors.append(f"{rid}: duplicate id (also in {seen[rid]})")
        seen[rid] = r.get("_file")
        if not ID_RE.match(str(rid)):
            errors.append(f"{rid}: id must be <PREFIX>-NNN")
        elif r.get("module") in PREFIX and not str(rid).startswith(PREFIX[r["module"]] + "-"):
            errors.append(f"{rid}: id prefix must be {PREFIX[r['module']]}- for module {r['module']}")
        if r.get("module") not in PREFIX:
            errors.append(f"{rid}: unknown module '{r.get('module')}'")
        for field, allowed in (("status", STATUS), ("priority", PRIORITY), ("type", RTYPE),
                               ("mode", MODE), ("cardinality", CARD), ("topology", TOPO), ("seam", SEAM)):
            v = r.get(field)
            if v is not None and v not in allowed:
                errors.append(f"{rid}: {field}='{v}' not in {sorted(allowed)}")
        if r.get("status") == "complete" and r.get("priority") == "MUST" \
                and r.get("type") in {"behavioral", "constraint"} and not r.get("tests"):
            errors.append(f"{rid}: MUST complete {r['type']} has no tests[]")
    return errors


def coverage(reqs: list[dict]) -> None:
    must = [r for r in reqs if r.get("priority") == "MUST" and r.get("status") != "rejected"]
    done = [r for r in must if r.get("status") == "complete" and r.get("tests")]
    print(f"\n=== Coverage (MUST) ===  {len(done)}/{len(must)} complete-with-tests")
    for r in must:
        if not (r.get("status") == "complete" and r.get("tests")):
            print(f"  GAP {r['id']:11} [{r['status']:10}] {r.get('group','')}/{r.get('category','')}")


def matrix(reqs: list[dict]) -> None:
    print("\n=== Status matrix (module / group) ===")
    by = {}
    for r in reqs:
        by.setdefault((r.get("module"), r.get("group")), {}).setdefault(r.get("status"), 0)
        by[(r.get("module"), r.get("group"))][r.get("status")] += 1
    for (mod, grp), counts in sorted(by.items(), key=lambda x: (x[0][0] or "", x[0][1] or "")):
        parts = " ".join(f"{s}:{n}" for s, n in sorted(counts.items()))
        print(f"  {mod:15} {str(grp):22} {parts}")


def orphan(reqs: list[dict]) -> None:
    cited = set()
    for r in reqs:
        for t in (r.get("tests") or []):
            cited.add(t.split("#")[0].strip())
    print("\n=== Orphan tests (under */src/test, not cited) — informational ===")
    total = uncited = 0
    for mod in PREFIX:
        for f in glob.glob(str(ROOT / mod / "src" / "test" / "**" / "*Test.java"), recursive=True):
            total += 1
            rel = Path(f).relative_to(ROOT).as_posix()
            if not any(rel.endswith(c) or c.endswith(Path(f).name) for c in cited):
                uncited += 1
    print(f"  {uncited}/{total} test files not yet cited by any requirement (expected high pre-migration)")


def triage(reqs: list[dict]) -> None:
    cmap_path = Path(__file__).resolve().parent / "coverage-map.yaml"
    if not cmap_path.exists():
        print("\n(no coverage-map.yaml)")
        return
    cmap = {e["id"]: e for e in (yaml.safe_load(cmap_path.read_text(encoding="utf-8")) or [])}
    file_ids = [r["id"] for r in reqs if r.get("module") == "file"]
    by_action = {"LINK": [], "RECODE": [], "WRITE": []}
    ungraded = []
    for rid in file_ids:
        e = cmap.get(rid)
        if not e:
            ungraded.append(rid)
        else:
            by_action.setdefault(e.get("action", "?"), []).append(rid)
    print("\n=== Triage (file adapter, from coverage-map.yaml) ===")
    for action in sorted(by_action):
        print(f"  {action:9} {len(by_action[action]):3}")
    print(f"  ungraded  {len(ungraded):3}  {ungraded if ungraded else ''}")
    print("\n  WRITE (genuine gaps — author from scratch):")
    for rid in by_action["WRITE"]:
        print(f"    {rid:11} {cmap[rid].get('note','')}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--coverage", action="store_true")
    ap.add_argument("--matrix", action="store_true")
    ap.add_argument("--orphan", action="store_true")
    ap.add_argument("--triage", action="store_true")
    args = ap.parse_args()

    reqs = load()
    errors = validate(reqs)
    by_status = {}
    for r in reqs:
        by_status[r.get("status")] = by_status.get(r.get("status"), 0) + 1
    print(f"Loaded {len(reqs)} requirements from {LEDGER_DIR.relative_to(ROOT)}/")
    for s, n in sorted(by_status.items()):
        print(f"  {s}: {n}")
    if errors:
        print(f"\nFAIL: {len(errors)} schema error(s):", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
    else:
        print("OK: schema valid")

    if args.coverage:
        coverage(reqs)
    if args.matrix:
        matrix(reqs)
    if args.orphan:
        orphan(reqs)
    if args.triage:
        triage(reqs)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
