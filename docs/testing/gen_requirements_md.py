#!/usr/bin/env python3
"""Generate docs/requirements/REQUIREMENTS.md (human-readable view) from the per-adapter ledgers.

Usage:
  python3 docs/testing/gen_requirements_md.py            # write the md
  python3 docs/testing/gen_requirements_md.py --check    # exit 1 if the committed md is stale (CI drift gate)
"""
from __future__ import annotations

import argparse
import glob
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    sys.exit("PyYAML required")

ROOT = Path(__file__).resolve().parents[2]
LEDGER_DIR = ROOT / "docs" / "requirements"
OUT = LEDGER_DIR / "REQUIREMENTS.md"
BADGE = {"complete": "[x]", "in-progress": "[~]", "accepted": "[ ]", "proposed": "[.]", "rejected": "[-]"}


def short(s: str, n: int = 110) -> str:
    s = " ".join((s or "").split())
    return s if len(s) <= n else s[: n - 1] + "…"


def generate() -> str:
    out = ["# Requirements — generated view",
           "",
           "> Generated from `docs/requirements/*.yaml` by `gen_requirements_md.py`. Do not hand-edit.",
           "> Status: `[x]` complete `[~]` in-progress `[ ]` accepted `[.]` proposed `[-]` rejected.",
           ""]
    total = 0
    for f in sorted(glob.glob(str(LEDGER_DIR / "*.yaml"))):
        reqs = yaml.safe_load(Path(f).read_text(encoding="utf-8")) or []
        if not reqs:
            continue
        module = reqs[0].get("module", Path(f).stem)
        counts = {}
        for r in reqs:
            counts[r.get("status")] = counts.get(r.get("status"), 0) + 1
        summary = ", ".join(f"{n} {s}" for s, n in sorted(counts.items()))
        out.append(f"## {module}  ({len(reqs)}: {summary})")
        out.append("")
        out.append("| | ID | Pri | Type | Group / Category | Guarantee | Tests |")
        out.append("|---|---|---|---|---|---|---|")
        for r in reqs:
            total += 1
            tests = ", ".join(r.get("tests") or []) or "—"
            out.append(
                f"| {BADGE.get(r.get('status'), '?')} | {r.get('id','')} | {r.get('priority','')} "
                f"| {r.get('type','')} | {r.get('group','')} / {r.get('category','')} "
                f"| {short(r.get('description',''))} | {short(tests, 60)} |")
        out.append("")
    out.insert(4, f"**{total} requirements across {len(glob.glob(str(LEDGER_DIR / '*.yaml')))} adapters.**\n")
    return "\n".join(out) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()
    content = generate()
    if args.check:
        if not OUT.exists() or OUT.read_text(encoding="utf-8") != content:
            print(f"DRIFT: {OUT.relative_to(ROOT)} is stale. Run gen_requirements_md.py", file=sys.stderr)
            return 1
        print("OK: REQUIREMENTS.md up to date")
        return 0
    OUT.write_text(content, encoding="utf-8")
    print(f"Wrote {OUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
